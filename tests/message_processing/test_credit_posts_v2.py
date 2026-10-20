"""
Credit posts under the v2 ledger: distribution and expense posts are inert,
transfers are settled by the fold (spec 5.6), and ledger inputs cannot be
forgotten.
"""

import datetime as dt
import hashlib
import json
from typing import Any, Dict, List, Optional

import pytest
from aleph_message.models import ItemHash
from configmanager import Config
from credit_v2_helpers import set_last_closed_period
from message_test_helpers import process_pending_messages
from more_itertools import one
from sqlalchemy import func, select

from aleph.db.accessors.messages import get_message_status
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb, PendingMessageDb
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_processing_result import ProcessedMessage, RejectedMessage
from aleph.types.message_status import ErrorCode, MessageStatus

CREDIT_SENDER = "0x2E4454fAD1906c0Ce6e45cBFA05cE898Ac3AC1dC"  # whitelisted (config)
USER = "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874"
RECIPIENT = "0x00000000000000000000000000000000000000Aa"
ATTESTOR = "0xAttestor0000000000000000000000000000000001"
PERIOD_START = 1_700_000_400 - (1_700_000_400 % 3600)
LAST_CLOSED = dt.datetime(2023, 11, 14, 20, tzinfo=dt.timezone.utc)


@pytest.fixture(autouse=True)
def attestor_config(mock_config: Config) -> None:
    # Read by the post handler at construction (message_processor fixture).
    mock_config.aleph.credits.attestation.addresses.value = [ATTESTOR]


def _enable_serve_v2(mock_config: Config, session_factory: DbSessionFactory) -> None:
    mock_config.aleph.credits.serve_v2.value = True
    # serve_v2 only takes effect once the fold has closed a period.
    with session_factory() as session:
        set_last_closed_period(session, LAST_CLOSED)
        session.commit()


@pytest.fixture
def serve_v2(mock_config: Config, session_factory: DbSessionFactory) -> Config:
    _enable_serve_v2(mock_config, session_factory)
    return mock_config


def _pending(
    *,
    msg_type: str,
    sender: str,
    channel: str,
    content: Dict[str, Any],
    nonce: float = 0.0,
) -> PendingMessageDb:
    message_time = 1_700_000_000.0 + nonce
    item_content = json.dumps({"address": sender, "time": message_time, **content})
    return PendingMessageDb(
        item_hash=hashlib.sha256(item_content.encode()).hexdigest(),
        type=msg_type,
        chain="ETH",
        sender=sender,
        signature="0xsig",
        item_type="inline",
        item_content=item_content,
        time=timestamp_to_datetime(message_time),
        channel=channel,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        check_message=False,
        fetched=True,
        reception_time=dt.datetime(2023, 1, 1),
    )


def _post(
    post_type: str,
    content: Optional[Any],
    *,
    sender: str = USER,
    channel: str = "ALEPH_CREDIT",
    nonce: float = 0.0,
) -> PendingMessageDb:
    body: Dict[str, Any] = {"type": post_type}
    if content is not None:
        body["content"] = content
    return _pending(
        msg_type="POST", sender=sender, channel=channel, content=body, nonce=nonce
    )


def _transfer(
    amount: Any = 100, recipient: str = RECIPIENT, nonce: float = 0.0
) -> PendingMessageDb:
    return _post(
        "aleph_credit_transfer",
        {"transfer": {"credits": [{"address": recipient, "amount": amount}]}},
        nonce=nonce,
    )


def _forget(target: PendingMessageDb, sender: str, channel: str) -> PendingMessageDb:
    return _pending(
        msg_type="FORGET",
        sender=sender,
        channel=channel,
        content={"hashes": [target.item_hash]},
        nonce=1000.0,
    )


async def _run(
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
    messages: List[PendingMessageDb],
):
    with session_factory() as session:
        return list(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=messages,
                session=session,
            )
        )


def _legacy_rows(session_factory: DbSessionFactory) -> int:
    with session_factory() as session:
        history = session.execute(
            select(func.count()).select_from(AlephCreditHistoryDb)
        ).scalar_one()
        lots = session.execute(
            select(func.count()).select_from(AlephCreditBalanceDb)
        ).scalar_one()
        return history + lots


@pytest.mark.asyncio
async def test_distribution_and_expense_posts_are_inert_under_v2(
    serve_v2: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    distribution = _post(
        "aleph_credit_distribution",
        {
            "distribution": {
                "credits": [
                    {
                        "address": USER,
                        "amount": 1000,
                        "price": "1.0",
                        "tx_hash": "0xtx",
                        "provider": "test",
                    }
                ],
                "token": "ALEPH",
                "chain": "ETH",
            }
        },
        sender=CREDIT_SENDER,
    )
    expense = _post(
        "aleph_credit_expense",
        {"expense": {"credits": [{"address": USER, "amount": 10}]}},
        sender=CREDIT_SENDER,
        nonce=1.0,
    )

    results = await _run(message_processor, session_factory, [distribution, expense])

    assert [type(r) for r in results] == [ProcessedMessage, ProcessedMessage]
    assert _legacy_rows(session_factory) == 0


@pytest.mark.asyncio
async def test_transfer_is_processed_even_when_balance_is_short_under_v2(
    serve_v2: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    transfer = _transfer(amount=10**12)

    result = one(await _run(message_processor, session_factory, [transfer]))

    assert isinstance(result, ProcessedMessage)
    with session_factory() as session:
        status = get_message_status(session, ItemHash(transfer.item_hash))
        assert status is not None and status.status == MessageStatus.PROCESSED
    # The fold settles it (or excludes it as insufficient_balance) later.
    assert _legacy_rows(session_factory) == 0


@pytest.mark.asyncio
async def test_transfer_with_short_balance_is_rejected_when_flag_off(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    assert mock_config.aleph.credits.serve_v2.value is False
    result = one(await _run(message_processor, session_factory, [_transfer()]))
    assert isinstance(result, RejectedMessage)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content",
    [
        None,
        {},
        {"transfer": {"credits": []}},
        {"transfer": {"credits": [{"address": RECIPIENT, "amount": 0}]}},
        {"transfer": {"credits": [{"address": RECIPIENT, "amount": "10"}]}},
        {"transfer": {"credits": [{"address": USER.lower(), "amount": 10}]}},
    ],
    ids=["missing", "empty", "no-entries", "zero", "string", "self-transfer"],
)
async def test_malformed_transfer_is_rejected_under_v2(
    serve_v2: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
    content: Optional[Dict[str, Any]],
):
    transfer = _post("aleph_credit_transfer", content)

    result = one(await _run(message_processor, session_factory, [transfer]))

    assert isinstance(result, RejectedMessage)
    assert result.error_code == ErrorCode.INVALID_FORMAT


@pytest.mark.asyncio
@pytest.mark.parametrize("serve", [False, True], ids=["legacy", "v2"])
async def test_forget_of_a_credit_transfer_is_refused(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
    serve: bool,
):
    if serve:
        _enable_serve_v2(mock_config, session_factory)
    else:
        # Fund the sender in the legacy ledger so ingest accepts the transfer.
        with session_factory() as session:
            session.add(
                AlephCreditBalanceDb(
                    address=USER,
                    credit_ref="grant",
                    credit_index=0,
                    amount_remaining=10**9,
                    expiration_date=None,
                    message_timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                    last_update=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                )
            )
            session.commit()
    transfer = _transfer()
    assert isinstance(
        one(await _run(message_processor, session_factory, [transfer])),
        ProcessedMessage,
    )

    result = one(
        await _run(
            message_processor,
            session_factory,
            [_forget(transfer, USER, "ALEPH_CREDIT")],
        )
    )

    assert isinstance(result, RejectedMessage)
    assert result.error_code == ErrorCode.FORGET_NOT_ALLOWED
    with session_factory() as session:
        status = get_message_status(session, ItemHash(transfer.item_hash))
        assert status is not None and status.status == MessageStatus.PROCESSED


@pytest.mark.asyncio
async def test_forget_of_a_whitelisted_attestation_is_refused(
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    attestation = _post(
        "aleph-execution-intervals",
        {
            "version": 1,
            "period": {"start": PERIOD_START, "end": PERIOD_START + 3600},
            "source": "test-attestor",
            "events_cursor": {"n": 0},
            "intervals": [],
            "totals": {"vms": 0},
        },
        # Listed case-insensitively.
        sender=ATTESTOR.lower(),
        channel="ALEPH-METERING",
    )
    assert isinstance(
        one(await _run(message_processor, session_factory, [attestation])),
        ProcessedMessage,
    )

    result = one(
        await _run(
            message_processor,
            session_factory,
            [_forget(attestation, ATTESTOR.lower(), "ALEPH-METERING")],
        )
    )

    assert isinstance(result, RejectedMessage)
    assert result.error_code == ErrorCode.FORGET_NOT_ALLOWED


@pytest.mark.asyncio
async def test_forget_of_an_unlisted_attestation_post_is_allowed(
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    other = "0xNotAnAttestor000000000000000000000000000001"
    post = _post(
        "aleph-execution-intervals",
        {"anything": True},
        sender=other,
        channel="ALEPH-METERING",
    )
    assert isinstance(
        one(await _run(message_processor, session_factory, [post])),
        ProcessedMessage,
    )

    result = one(
        await _run(
            message_processor,
            session_factory,
            [_forget(post, other, "ALEPH-METERING")],
        )
    )

    assert isinstance(result, ProcessedMessage)


@pytest.mark.asyncio
async def test_transfer_follows_the_flag_before_the_first_closed_period(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    """
    serve_v2 on a node whose fold never closed a period: reads stay legacy,
    but the ingest verdict on a transfer follows the flag, so every node
    processes it whatever the state of its fold.
    """
    mock_config.aleph.credits.serve_v2.value = True
    transfer = _transfer(amount=10**12)

    result = one(await _run(message_processor, session_factory, [transfer]))

    assert isinstance(result, ProcessedMessage)
    assert _legacy_rows(session_factory) == 0


@pytest.mark.asyncio
async def test_distribution_is_inert_under_the_flag_before_the_first_closed_period(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    mock_config.aleph.credits.serve_v2.value = True
    distribution = _post(
        "aleph_credit_distribution",
        {
            "distribution": {
                "credits": [
                    {
                        "address": USER,
                        "amount": 1000,
                        "price": "1.0",
                        "tx_hash": "0xtx",
                        "provider": "test",
                    }
                ],
                "token": "ALEPH",
                "chain": "ETH",
            }
        },
        sender=CREDIT_SENDER,
    )

    result = one(await _run(message_processor, session_factory, [distribution]))

    assert isinstance(result, ProcessedMessage)
    assert _legacy_rows(session_factory) == 0


@pytest.mark.asyncio
async def test_malformed_distribution_is_rejected_under_the_flag(
    serve_v2: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    distribution = _post(
        "aleph_credit_distribution",
        {"distribution": {"credits": "nope"}},
        sender=CREDIT_SENDER,
    )

    result = one(await _run(message_processor, session_factory, [distribution]))

    assert isinstance(result, RejectedMessage)
    assert _legacy_rows(session_factory) == 0
