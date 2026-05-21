import datetime as dt

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from sqlalchemy import select

from aleph.db.models import CcnMetricDb, CrnMetricDb, MessageDb
from aleph.handlers.content.post import PostMessageHandler
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory

SCORING_SENDER = "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4"
SCORING_CHANNEL = "aleph-scoring"
SCORING_POST_TYPE = "aleph-network-metrics"


def _make_scoring_message(
    item_hash: str, content_type: str, address: str, channel: str
) -> MessageDb:
    payload = {
        "type": content_type,
        "address": address,
        "time": 1700000000.0,
        "content": {
            "metrics": {
                "crn": [
                    {
                        "measured_at": 1700000000.0,
                        "node_id": "crn-A",
                        "base_latency": 0.1,
                    },
                ],
                "ccn": [
                    {
                        "measured_at": 1700000000.0,
                        "node_id": "ccn-A",
                        "pending_messages": 3,
                    },
                ],
            },
        },
    }
    return MessageDb(
        item_hash=item_hash,
        type=MessageType.post,
        chain=Chain.ETH,
        sender=address,
        channel=Channel(channel),
        signature=None,
        item_type=ItemType.inline,
        item_content="{}",
        content=payload,
        time=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        size=2,
    )


def _handler() -> PostMessageHandler:
    return PostMessageHandler(
        balances_addresses=[],
        balances_post_type="no-balances-today",
        credit_balances_addresses=[],
        credit_balances_post_types=["no-credit-balances-today"],
        credit_balances_channels=["nope"],
        scoring_addresses=[SCORING_SENDER],
        scoring_channel=SCORING_CHANNEL,
        scoring_metrics_post_type=SCORING_POST_TYPE,
    )


@pytest.mark.asyncio
async def test_scoring_post_inserts_metric_rows(session_factory: DbSessionFactory):
    handler = _handler()
    msg = _make_scoring_message(
        item_hash="hash-1",
        content_type=SCORING_POST_TYPE,
        address=SCORING_SENDER,
        channel=SCORING_CHANNEL,
    )
    with session_factory() as session:
        session.add(msg)
        session.flush()
        await handler.process(session=session, messages=[msg])
        session.commit()

        crn = list(session.execute(select(CrnMetricDb)).scalars())
        ccn = list(session.execute(select(CcnMetricDb)).scalars())

    assert [(r.item_hash, r.node_id, r.base_latency) for r in crn] == [
        ("hash-1", "crn-A", 0.1)
    ]
    assert [(r.item_hash, r.node_id, r.pending_messages) for r in ccn] == [
        ("hash-1", "ccn-A", 3)
    ]


@pytest.mark.asyncio
async def test_non_scoring_post_type_does_not_insert_metrics(
    session_factory: DbSessionFactory,
):
    handler = _handler()
    msg = _make_scoring_message(
        item_hash="hash-2",
        content_type="something-else",
        address=SCORING_SENDER,
        channel=SCORING_CHANNEL,
    )
    with session_factory() as session:
        session.add(msg)
        session.flush()
        await handler.process(session=session, messages=[msg])
        session.commit()

        assert list(session.execute(select(CrnMetricDb)).scalars()) == []
        assert list(session.execute(select(CcnMetricDb)).scalars()) == []


@pytest.mark.asyncio
async def test_non_allowlisted_sender_does_not_insert_metrics(
    session_factory: DbSessionFactory,
):
    handler = _handler()
    msg = _make_scoring_message(
        item_hash="hash-3",
        content_type=SCORING_POST_TYPE,
        address="0xSomeoneElse00000000000000000000000000",
        channel=SCORING_CHANNEL,
    )
    with session_factory() as session:
        session.add(msg)
        session.flush()
        await handler.process(session=session, messages=[msg])
        session.commit()

        assert list(session.execute(select(CrnMetricDb)).scalars()) == []
        assert list(session.execute(select(CcnMetricDb)).scalars()) == []


@pytest.mark.asyncio
async def test_wrong_channel_does_not_insert_metrics(
    session_factory: DbSessionFactory,
):
    handler = _handler()
    msg = _make_scoring_message(
        item_hash="hash-4",
        content_type=SCORING_POST_TYPE,
        address=SCORING_SENDER,
        channel="wrong-channel",
    )
    with session_factory() as session:
        session.add(msg)
        session.flush()
        await handler.process(session=session, messages=[msg])
        session.commit()

        assert list(session.execute(select(CrnMetricDb)).scalars()) == []
        assert list(session.execute(select(CcnMetricDb)).scalars()) == []


@pytest.mark.asyncio
async def test_forget_cascades_to_metric_rows(session_factory: DbSessionFactory):
    handler = _handler()
    msg = _make_scoring_message(
        item_hash="hash-forget",
        content_type=SCORING_POST_TYPE,
        address=SCORING_SENDER,
        channel=SCORING_CHANNEL,
    )
    with session_factory() as session:
        session.add(msg)
        session.flush()
        await handler.process(session=session, messages=[msg])
        session.commit()

        # Sanity: rows are there
        assert (
            len(
                list(
                    session.execute(
                        select(CrnMetricDb).where(
                            CrnMetricDb.item_hash == "hash-forget"
                        )
                    ).scalars()
                )
            )
            == 1
        )
        assert (
            len(
                list(
                    session.execute(
                        select(CcnMetricDb).where(
                            CcnMetricDb.item_hash == "hash-forget"
                        )
                    ).scalars()
                )
            )
            == 1
        )

    # Now delete the source message and verify cascade.
    with session_factory() as session:
        existing = session.get(MessageDb, "hash-forget")
        assert existing is not None
        session.delete(existing)
        session.commit()

        assert (
            list(
                session.execute(
                    select(CrnMetricDb).where(CrnMetricDb.item_hash == "hash-forget")
                ).scalars()
            )
            == []
        )
        assert (
            list(
                session.execute(
                    select(CcnMetricDb).where(CcnMetricDb.item_hash == "hash-forget")
                ).scalars()
            )
            == []
        )
