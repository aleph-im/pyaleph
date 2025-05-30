import datetime as dt
from copy import copy

import pytest
import pytz
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from sqlalchemy import insert, select, text

from aleph.db.accessors.messages import (
    append_to_forgotten_by,
    forget_message,
    get_distinct_channels,
    get_forgotten_message,
    get_message_by_item_hash,
    get_message_status,
    get_unconfirmed_messages,
    make_confirmation_upsert_query,
    make_message_upsert_query,
    message_exists,
)
from aleph.db.models import ChainTxDb, MessageDb, MessageStatusDb, message_confirmations
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.channel import Channel
from aleph.types.db_session import AsyncDbSessionFactory
from aleph.types.message_status import MessageStatus


@pytest.fixture
def fixture_message() -> MessageDb:
    # TODO: use a valid message, this one has incorrect signature, size, etc.

    sender = "0x51A58800b26AA1451aaA803d1746687cB88E0500"
    return MessageDb(
        item_hash="aea68aac5f4dc6e6b813fc5de9e6c17d3ef1b03e77eace15398405260baf3ce4",
        chain=Chain.ETH,
        sender=sender,
        signature="0x705ca1365a0b794cbfcf89ce13239376d0aab0674d8e7f39965590a46e5206a664bc4b313f3351f313564e033c9fe44fd258492dfbd6c36b089677d73224da0a1c",
        type=MessageType.aggregate,
        item_content='{"address": "0x51A58800b26AA1451aaA803d1746687cB88E0500", "key": "my-aggregate", "time": 1664999873, "content": {"easy": "as", "a-b": "c"}}',
        content={
            "address": sender,
            "key": "my-aggregate",
            "time": 1664999873,
            "content": {"easy": "as", "a-b": "c"},
        },
        item_type=ItemType.inline,
        size=2000,
        time=pytz.utc.localize(dt.datetime.utcfromtimestamp(1664999872)),
        channel=Channel("CHANEL-N5"),
    )


def assert_messages_equal(expected: MessageDb, actual: MessageDb):
    assert actual.item_hash == expected.item_hash
    assert actual.chain == expected.chain
    assert actual.sender == expected.sender
    assert actual.signature == expected.signature
    assert actual.type == expected.type
    assert actual.content == expected.content
    assert actual.item_type == expected.item_type
    assert actual.size == expected.size
    assert actual.time == expected.time
    assert actual.channel == expected.channel


@pytest.mark.asyncio
async def test_get_message(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    async with session_factory() as session:
        session.add(fixture_message)
        await session.commit()

    async with session_factory() as session:
        fetched_message = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )

    assert fetched_message is not None
    assert_messages_equal(expected=fixture_message, actual=fetched_message)

    # Check confirmation fields/properties
    assert fetched_message.confirmations == []
    assert not fetched_message.confirmed


@pytest.mark.asyncio
async def test_get_message_with_confirmations(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    confirmations = [
        ChainTxDb(
            hash="0xdeadbeef",
            chain=Chain.ETH,
            height=1000,
            datetime=pytz.utc.localize(dt.datetime(2022, 10, 1)),
            publisher="0xabadbabe",
            protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
            protocol_version=1,
            content="tx-content-1",
        ),
        ChainTxDb(
            hash="0x8badf00d",
            chain=Chain.ETH,
            height=1020,
            datetime=pytz.utc.localize(dt.datetime(2022, 10, 2)),
            publisher="0x0bobafed",
            protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
            protocol_version=1,
            content="tx-content-2",
        ),
    ]

    fixture_message.confirmations = confirmations

    async with session_factory() as session:
        session.add(fixture_message)
        await session.commit()

    async with session_factory() as session:
        fetched_message = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )

    assert fetched_message is not None
    assert_messages_equal(expected=fixture_message, actual=fetched_message)

    assert fetched_message.confirmed

    confirmations_by_hash = {
        confirmation.hash: confirmation for confirmation in confirmations
    }
    for confirmation in fetched_message.confirmations:
        original = confirmations_by_hash[confirmation.hash]
        assert confirmation.hash == original.hash
        assert confirmation.chain == original.chain
        assert confirmation.height == original.height
        assert confirmation.datetime == original.datetime
        assert confirmation.publisher == original.publisher


@pytest.mark.asyncio
async def test_message_exists(session_factory: AsyncDbSessionFactory, fixture_message):
    async with session_factory() as session:
        assert not message_exists(session=session, item_hash=fixture_message.item_hash)

        session.add(fixture_message)
        await session.commit()

        assert message_exists(session=session, item_hash=fixture_message.item_hash)


@pytest.mark.asyncio
async def test_message_count(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    async with session_factory() as session:
        session.add(fixture_message)
        await session.commit()

        # Analyze updates the table size estimate
        await session.execute(text("analyze messages"))
        await session.commit()

    async with session_factory() as session:
        exact_count = await MessageDb.count(session)
        assert exact_count == 1

        estimate_count = await MessageDb.estimate_count(session)
        assert isinstance(estimate_count, int)
        assert estimate_count == 1

        fast_count = await MessageDb.fast_count(session)
        assert fast_count == 1


@pytest.mark.asyncio
async def test_upsert_query_confirmation(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    item_hash = fixture_message.item_hash

    chain_tx = ChainTxDb(
        hash="0xdeadbeef",
        chain=Chain.ETH,
        height=1000,
        datetime=pytz.utc.localize(dt.datetime(2022, 10, 1)),
        publisher="0xabadbabe",
        protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
        protocol_version=1,
        content="Qmsomething",
    )

    upsert_stmt = make_confirmation_upsert_query(
        item_hash=item_hash, tx_hash=chain_tx.hash
    )

    async with session_factory() as session:
        session.add(fixture_message)
        session.add(chain_tx)
        await session.commit()

    # Insert
    async with session_factory() as session:
        await session.execute(upsert_stmt)
        await session.commit()

        confirmation_db = (
            await session.execute(
                select(message_confirmations).where(
                    message_confirmations.c.item_hash == item_hash
                )
            )
        ).one()
        assert confirmation_db.tx_hash == chain_tx.hash

    # Upsert
    async with session_factory() as session:
        await session.execute(upsert_stmt)
        await session.commit()

        confirmation_db = (
            await session.execute(
                select(message_confirmations).where(
                    message_confirmations.c.item_hash == item_hash
                )
            )
        ).one()
        assert confirmation_db.tx_hash == chain_tx.hash


@pytest.mark.asyncio
async def test_upsert_query_message(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    message = copy(fixture_message)
    message.time = fixture_message.time - dt.timedelta(seconds=1)

    upsert_stmt = make_message_upsert_query(message)

    async with session_factory() as session:
        session.add(message)
        await session.commit()

    async with session_factory() as session:
        await session.execute(upsert_stmt)
        await session.commit()

        message_db = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(message.item_hash)
        )

    assert message_db
    assert message_db.time == message.time


@pytest.mark.asyncio
async def test_get_unconfirmed_messages(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    async with session_factory() as session:
        session.add(fixture_message)
        session.add(
            MessageStatusDb(
                item_hash=fixture_message.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=fixture_message.time,
            )
        )
        await session.commit()

    async with session_factory() as session:
        unconfirmed_messages = list(await get_unconfirmed_messages(session))

    assert len(unconfirmed_messages) == 1
    assert_messages_equal(fixture_message, unconfirmed_messages[0])

    # Confirm the message and check that it is not returned anymore
    tx = ChainTxDb(
        hash="1234",
        chain=Chain.SOL,
        height=8000,
        datetime=timestamp_to_datetime(1664999900),
        publisher="0xabadbabe",
        protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
        protocol_version=1,
        content="Qmsomething",
    )
    async with session_factory() as session:
        session.add(tx)
        await session.flush()
        await session.execute(
            insert(message_confirmations).values(
                item_hash=fixture_message.item_hash, tx_hash=tx.hash
            )
        )
        await session.commit()

    async with session_factory() as session:
        # Check that the message is now ignored
        unconfirmed_messages = list(await get_unconfirmed_messages(session))
        assert unconfirmed_messages == []

        # Check that it is also ignored when the chain parameter is specified
        unconfirmed_messages = list(
            await get_unconfirmed_messages(session, chain=tx.chain)
        )
        assert unconfirmed_messages == []

        # Check that it reappears if we specify a different chain
        unconfirmed_messages = list(
            await get_unconfirmed_messages(session, chain=Chain.TEZOS)
        )
        assert len(unconfirmed_messages) == 1
        assert_messages_equal(fixture_message, unconfirmed_messages[0])

        # Check that the limit parameter is respected
        unconfirmed_messages = list(
            await get_unconfirmed_messages(session, chain=Chain.TEZOS, limit=0)
        )
        assert unconfirmed_messages == []


@pytest.mark.asyncio
async def test_get_unconfirmed_messages_trusted_messages(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    fixture_message.signature = None
    async with session_factory() as session:
        session.add(fixture_message)
        await session.commit()

    async with session_factory() as session:
        unconfirmed_messages = list(await get_unconfirmed_messages(session))
        assert unconfirmed_messages == []


@pytest.mark.asyncio
async def test_get_distinct_channels(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    # TODO: improve this test
    #       * use several messages
    #       * test if None if considered as a channel
    #       * test
    async with session_factory() as session:
        session.add(fixture_message)
        await session.commit()
        channels = list(await get_distinct_channels(session=session))

    assert channels == [fixture_message.channel]


@pytest.mark.asyncio
async def test_forget_message(
    session_factory: AsyncDbSessionFactory, fixture_message: MessageDb
):
    async with session_factory() as session:
        session.add(fixture_message)
        session.add(
            MessageStatusDb(
                item_hash=fixture_message.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=fixture_message.time,
            )
        )
        await session.commit()

    forget_message_hash = (
        "d06251c954d4c75476c749e80b8f2a4962d20282b28b3e237e30b0a76157df2d"
    )

    async with session_factory() as session:
        await forget_message(
            session=session,
            item_hash=fixture_message.item_hash,
            forget_message_hash=forget_message_hash,
        )
        await session.commit()

        message_status = await get_message_status(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )
        assert message_status
        assert message_status.status == MessageStatus.FORGOTTEN

        # Assert that the message is not present in messages anymore
        message = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )
        assert message is None

        # Assert that the metadata was inserted properly in forgotten_messages
        forgotten_message = await get_forgotten_message(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )
        assert forgotten_message

        assert forgotten_message.item_hash == fixture_message.item_hash
        assert forgotten_message.type == fixture_message.type
        assert forgotten_message.chain == fixture_message.chain
        assert forgotten_message.sender == fixture_message.sender
        assert forgotten_message.signature == fixture_message.signature
        assert forgotten_message.item_type == fixture_message.item_type
        assert forgotten_message.time == fixture_message.time
        assert forgotten_message.channel == fixture_message.channel
        assert forgotten_message.forgotten_by == [forget_message_hash]

        # Now, add a hash to forgotten_by
        new_forget_message_hash = (
            "2aa1f44199181110e0c6b79ccc5e40ceaf20eac791dcfcd1b4f8f2f32b2d8502"
        )

        await append_to_forgotten_by(
            session=session,
            forgotten_message_hash=fixture_message.item_hash,
            forget_message_hash=new_forget_message_hash,
        )
        await session.commit()

        forgotten_message = await get_forgotten_message(
            session=session, item_hash=fixture_message.item_hash
        )
        assert forgotten_message
        assert forgotten_message.forgotten_by == [
            forget_message_hash,
            new_forget_message_hash,
        ]
