import datetime as dt
from decimal import Decimal

import pytest
import pytest_asyncio
from aleph_message.models import Chain, ItemType, MessageType, PaymentType

from aleph.db.accessors.messages import get_message_status
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.chains import ChainTxDb
from aleph.db.models.cron_jobs import CronJobDb
from aleph.db.models.messages import MessageDb, MessageStatusDb
from aleph.jobs.cron.credit_balance_job import CreditBalanceCronJob
from aleph.toolkit.constants import DAY, DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus


@pytest.fixture
def credit_job(session_factory: DbSessionFactory) -> CreditBalanceCronJob:
    return CreditBalanceCronJob(
        session_factory=session_factory,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )


@pytest.fixture
def now():
    return utc_now()


def _create_cron_job(id, now):
    return CronJobDb(
        id=id,
        interval=1,
        last_run=now - dt.timedelta(hours=1),
    )


def _create_credit_instance(
    session_factory,
    now,
    *,
    address,
    item_hash,
    cost_credit_per_second,
):
    """Create a credit-paid INSTANCE message with a PROCESSED status and a per-second
    credit cost record. The address balance is created separately so several
    instances can share one balance."""
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

    chain_tx = ChainTxDb(
        hash=f"0xtx_{item_hash[:8]}",
        chain=Chain.ETH,
        height=1_000_000,
        datetime=now,
        publisher="0xabadbabe",
        protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
        protocol_version=1,
        content="Qmsomething",
    )
    message.confirmations = [chain_tx]

    message_status = MessageStatusDb(
        item_hash=item_hash,
        status=MessageStatus.PROCESSED,
        reception_time=now,
    )

    cost = AccountCostsDb(
        owner=address,
        item_hash=item_hash,
        type=CostType.EXECUTION,
        name="instance",
        payment_type=PaymentType.credit,
        cost_hold=Decimal("0"),
        cost_stream=Decimal("0"),
        cost_credit=Decimal(str(cost_credit_per_second)),
    )

    with session_factory() as session:
        session.add(message)
        session.commit()
        session.add_all([message_status, cost])
        session.commit()


def _create_credit_balance(session_factory, now, *, address, amount, ref="grant"):
    """Give ``address`` a single non-expiring credit lot of ``amount`` plus a fresh
    credit-history row so get_updated_credit_balance_accounts surfaces the account."""
    credit_lot = AlephCreditBalanceDb(
        address=address,
        credit_ref=ref,
        credit_index=0,
        amount_remaining=int(amount),
        expiration_date=None,
        message_timestamp=now - dt.timedelta(days=1),
        last_update=now,
    )
    credit_history = AlephCreditHistoryDb(
        address=address,
        amount=int(amount),
        credit_ref=ref,
        credit_index=0,
        expiration_date=None,
        message_timestamp=now - dt.timedelta(days=1),
        last_update=now,
    )
    with session_factory() as session:
        session.add_all([credit_lot, credit_history])
        session.commit()


@pytest_asyncio.fixture
async def fixture_base_cron(session_factory, now):
    with session_factory() as session:
        session.add(_create_cron_job("credit_check_base", now))
        session.commit()
    return "credit_check_base"


@pytest.mark.asyncio
async def test_credit_job_removes_instance_that_cannot_afford_one_day(
    session_factory, credit_job, fixture_base_cron, now
):
    """An instance whose credit balance covers far less than one day of runtime must
    be marked for removal.

    Costs in account_costs are stored per-second. With a per-second cost of 1 credit,
    one day of runtime requires ``1 * DAY`` (86400) credits. A balance of 3600 credits
    (one hour) is therefore insufficient and the instance must enter REMOVING.

    This fails while the cron multiplies the per-second cost by 24 (treating it as an
    hourly rate): the threshold becomes 24 credits, 3600 >= 24, and the instance is
    wrongly kept PROCESSED.
    """
    address = "0xcreditunderfunded"
    item_hash = "ab" * 32

    _create_credit_instance(
        session_factory,
        now,
        address=address,
        item_hash=item_hash,
        cost_credit_per_second=1,
    )
    _create_credit_balance(
        session_factory, now, address=address, amount=3600
    )  # one hour of runtime

    with session_factory() as session:
        cron_job = session.query(CronJobDb).filter_by(id=fixture_base_cron).one()

    await credit_job.run(now, cron_job)

    with session_factory() as session:
        status = get_message_status(session=session, item_hash=item_hash)
        assert status is not None
        assert status.status == MessageStatus.REMOVING


@pytest.mark.asyncio
async def test_credit_job_keeps_instance_that_can_afford_one_day(
    session_factory, credit_job, fixture_base_cron, now
):
    """An instance whose credit balance comfortably covers more than one day of runtime
    must stay PROCESSED (guard against over-removal after the unit fix)."""
    address = "0xcreditfunded"
    item_hash = "cd" * 32

    _create_credit_instance(
        session_factory,
        now,
        address=address,
        item_hash=item_hash,
        cost_credit_per_second=1,
    )
    _create_credit_balance(
        session_factory, now, address=address, amount=2 * DAY
    )  # two days of runtime

    with session_factory() as session:
        cron_job = session.query(CronJobDb).filter_by(id=fixture_base_cron).one()

    await credit_job.run(now, cron_job)

    with session_factory() as session:
        status = get_message_status(session=session, item_hash=item_hash)
        assert status is not None
        assert status.status == MessageStatus.PROCESSED


@pytest.mark.asyncio
async def test_credit_job_reserves_a_full_day_per_instance(
    session_factory, credit_job, fixture_base_cron, now
):
    """The running-balance check must reserve one full day of runtime per instance,
    not one second.

    Two instances cost 1 credit/second each (86400/day each, 172800/day combined).
    The address holds 90000 credits: enough for a full day of ONE instance, but not
    both. Exactly one instance must be removed.

    This fails when the loop decrements the running balance by the per-second cost
    instead of the per-day cost: after the first instance the balance barely drops
    (90000 -> 89999), so the second instance is also compared against ~90000 >= 86400
    and wrongly kept, leaving the account running two instances it cannot afford for a
    day -- exactly what the ingest-time check (sum of costs * DAY) would have rejected.
    """
    address = "0xcredittwoinstances"
    item_hash_a = "a1" * 32
    item_hash_b = "b2" * 32

    _create_credit_instance(
        session_factory,
        now,
        address=address,
        item_hash=item_hash_a,
        cost_credit_per_second=1,
    )
    _create_credit_instance(
        session_factory,
        now,
        address=address,
        item_hash=item_hash_b,
        cost_credit_per_second=1,
    )
    # One full day for a single instance plus a margin, far below two days.
    _create_credit_balance(session_factory, now, address=address, amount=DAY + 3600)

    with session_factory() as session:
        cron_job = session.query(CronJobDb).filter_by(id=fixture_base_cron).one()

    await credit_job.run(now, cron_job)

    with session_factory() as session:
        statuses = {
            get_message_status(session=session, item_hash=h).status
            for h in (item_hash_a, item_hash_b)
        }

    # Exactly one instance affordable for a full day, the other removed.
    assert statuses == {MessageStatus.PROCESSED, MessageStatus.REMOVING}
