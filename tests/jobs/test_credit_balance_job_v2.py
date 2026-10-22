"""The credit removal cron reads the v2 ledger under `serve_v2`."""

import datetime as dt
import logging
from decimal import Decimal

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType, PaymentType
from configmanager import Config
from credit_v2_helpers import seed_instance, seed_lot, set_last_closed_period

from aleph.db.accessors.messages import get_message_status
from aleph.db.models import AlephCreditHistoryDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.chains import ChainTxDb
from aleph.db.models.credit_ledger import CreditLedgerDb
from aleph.db.models.cron_jobs import CronJobDb
from aleph.db.models.messages import MessageDb, MessageStatusDb
from aleph.jobs.cron.credit_balance_job import CreditBalanceCronJob
from aleph.services.credits.balance import addresses_with_ledger_activity_since
from aleph.services.credits.periods import floor_hour
from aleph.toolkit.constants import DAY, DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

POOR = "0xAbCdEf0000000000000000000000000000000001"
FUNDED = "0xAbCdEf0000000000000000000000000000000002"
LEGACY_ONLY = "0xAbCdEf0000000000000000000000000000000003"
STALE = "0xAbCdEf0000000000000000000000000000000004"


@pytest.fixture
def serve_v2(mock_config: Config) -> Config:
    mock_config.aleph.credits.serve_v2.value = True
    return mock_config


def _credit_instance(session, now: dt.datetime, address: str, item_hash: str):
    message = MessageDb(
        item_hash=item_hash,
        sender=address,
        chain=Chain.ETH,
        type=MessageType.instance,
        time=now,
        item_type=ItemType.inline,
        signature=f"sig_{item_hash[:8]}",
        size=1000,
        content={
            "address": address,
            "time": now.timestamp(),
            "payment": {"type": "credit", "chain": "ETH"},
        },
    )
    message.confirmations = [
        ChainTxDb(
            hash=f"0xtx_{item_hash[:8]}",
            chain=Chain.ETH,
            height=1_000_000,
            datetime=now,
            publisher="0xabadbabe",
            protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
            protocol_version=1,
            content="Qmsomething",
        )
    ]
    session.add(message)
    session.flush()
    session.add_all(
        [
            MessageStatusDb(
                item_hash=item_hash, status=MessageStatus.PROCESSED, reception_time=now
            ),
            AccountCostsDb(
                owner=address,
                item_hash=item_hash,
                type=CostType.EXECUTION,
                name="instance",
                payment_type=PaymentType.credit,
                cost_hold=Decimal("0"),
                cost_stream=Decimal("0"),
                cost_credit=Decimal("1"),
            ),
        ]
    )
    session.flush()


def _activity(session, address: str, period: dt.datetime, key: str) -> None:
    session.add(
        CreditLedgerDb(
            period=period,
            address=address,
            kind="execution",
            business_key=key,
            amount=0,
            excluded_reason="no_attestation",
        )
    )
    session.flush()


def _status(session_factory: DbSessionFactory, item_hash: str) -> MessageStatus:
    with session_factory() as session:
        status = get_message_status(session=session, item_hash=ItemHash(item_hash))
        assert status is not None
        return status.status


@pytest.mark.asyncio
async def test_credit_balance_cron_picks_addresses_with_ledger_rows(
    session_factory: DbSessionFactory, serve_v2: Config
):
    now = utc_now()
    last_closed = floor_hour(now) - dt.timedelta(hours=1)
    with session_factory() as session:
        set_last_closed_period(session, last_closed)
        cron = CronJobDb(
            id="credit_v2", interval=1, last_run=now - dt.timedelta(hours=1)
        )
        session.add(cron)

        # Ledger addresses are lowercase; the messages use the checksummed form.
        _credit_instance(session, now, POOR, "a1" * 32)
        _activity(session, POOR.lower(), last_closed, "p:poor")

        _credit_instance(session, now, FUNDED, "a2" * 32)
        seed_lot(session, FUNDED.lower(), 2 * DAY, period=last_closed, key="t:f")

        # Updated legacy history only: not a v2 candidate.
        _credit_instance(session, now, LEGACY_ONLY, "a3" * 32)
        session.add(
            AlephCreditHistoryDb(
                address=LEGACY_ONLY,
                amount=1,
                credit_ref="legacy",
                credit_index=0,
                message_timestamp=now,
                last_update=now,
            )
        )

        # Ledger activity too old to be picked by this run.
        _credit_instance(session, now, STALE, "a4" * 32)
        _activity(session, STALE.lower(), last_closed - dt.timedelta(days=2), "p:old")
        session.commit()

        assert addresses_with_ledger_activity_since(
            session, now - dt.timedelta(hours=6)
        ) == sorted([POOR.lower(), FUNDED.lower()])

    job = CreditBalanceCronJob(
        session_factory=session_factory,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )
    with session_factory() as session:
        loaded = session.get(CronJobDb, "credit_v2")
    assert loaded is not None
    assert await job.run(now, loaded)

    assert _status(session_factory, "a1" * 32) == MessageStatus.REMOVING
    assert _status(session_factory, "a2" * 32) == MessageStatus.PROCESSED
    assert _status(session_factory, "a3" * 32) == MessageStatus.PROCESSED
    assert _status(session_factory, "a4" * 32) == MessageStatus.PROCESSED


@pytest.mark.asyncio
async def test_stalled_fold_is_logged_and_running_resources_are_still_checked(
    session_factory: DbSessionFactory, serve_v2: Config, caplog
):
    now = utc_now()
    with session_factory() as session:
        # Far beyond grace (3 h) + 2 h.
        set_last_closed_period(session, floor_hour(now) - dt.timedelta(hours=12))
        session.add(
            CronJobDb(id="credit_v2", interval=1, last_run=now - dt.timedelta(hours=1))
        )
        # A running credit VM with no ledger row at all and no lots.
        _credit_instance(session, now, POOR, "b1" * 32)
        seed_instance(
            session,
            "b1" * 32,
            owner=POOR.lower(),
            first_confirmed_at=now - dt.timedelta(days=1),
        )
        session.commit()

    job = CreditBalanceCronJob(
        session_factory=session_factory,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )
    with session_factory() as session:
        loaded = session.get(CronJobDb, "credit_v2")
    assert loaded is not None
    with caplog.at_level(logging.ERROR, logger="aleph.jobs.cron.credit_balance_job"):
        assert await job.run(now, loaded)

    stalled = [
        r
        for r in caplog.records
        if r.levelno == logging.ERROR and "stalled" in r.getMessage()
    ]
    assert len(stalled) == 1
    assert _status(session_factory, "b1" * 32) == MessageStatus.REMOVING
