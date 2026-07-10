import datetime as dt
from copy import copy
from typing import Any, Dict

import pytest
import pytz
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from message_test_helpers import make_validated_message_from_dict
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
from aleph.db.models import (
    ChainTxDb,
    ForgottenMessageDb,
    MessageDb,
    MessageStatusDb,
    StoredFileDb,
    message_confirmations,
)
from aleph.db.models.messages import RemovedMessageDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
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
        time=dt.datetime.fromtimestamp(1664999872, dt.timezone.utc),
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
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    with session_factory() as session:
        session.add(fixture_message)
        session.commit()

    with session_factory() as session:
        fetched_message = get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )

    assert fetched_message is not None
    assert_messages_equal(expected=fixture_message, actual=fetched_message)

    # Check confirmation fields/properties
    assert fetched_message.confirmations == []
    assert not fetched_message.confirmed


@pytest.mark.asyncio
async def test_get_message_with_confirmations(
    session_factory: DbSessionFactory, fixture_message: MessageDb
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

    with session_factory() as session:
        session.add(fixture_message)
        session.commit()

    with session_factory() as session:
        fetched_message = get_message_by_item_hash(
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
async def test_message_exists(session_factory: DbSessionFactory, fixture_message):
    with session_factory() as session:
        assert not message_exists(session=session, item_hash=fixture_message.item_hash)

        session.add(fixture_message)
        session.commit()

        assert message_exists(session=session, item_hash=fixture_message.item_hash)


@pytest.mark.asyncio
async def test_message_count(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    with session_factory() as session:
        session.add(fixture_message)
        session.commit()

        # Analyze updates the table size estimate
        session.execute(text("analyze messages"))
        session.commit()

    with session_factory() as session:
        exact_count = MessageDb.count(session)
        assert exact_count == 1

        estimate_count = MessageDb.estimate_count(session)
        assert isinstance(estimate_count, int)
        assert estimate_count == 1

        fast_count = MessageDb.fast_count(session)
        assert fast_count == 1


@pytest.mark.asyncio
async def test_upsert_query_confirmation(
    session_factory: DbSessionFactory, fixture_message: MessageDb
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

    with session_factory() as session:
        session.add(fixture_message)
        session.add(chain_tx)
        session.commit()

    # Insert
    with session_factory() as session:
        session.execute(upsert_stmt)
        session.commit()

        confirmation_db = session.execute(
            select(message_confirmations).where(
                message_confirmations.c.item_hash == item_hash
            )
        ).one()
        assert confirmation_db.tx_hash == chain_tx.hash

        # Trigger should have populated denormalized confirmation columns
        message = get_message_by_item_hash(
            session=session, item_hash=ItemHash(item_hash)
        )
        assert message is not None
        assert message.first_confirmed_at == chain_tx.datetime
        assert message.first_confirmed_height == chain_tx.height

    # Upsert
    with session_factory() as session:
        session.execute(upsert_stmt)
        session.commit()

        confirmation_db = session.execute(
            select(message_confirmations).where(
                message_confirmations.c.item_hash == item_hash
            )
        ).one()
        assert confirmation_db.tx_hash == chain_tx.hash


@pytest.mark.asyncio
async def test_confirmation_trigger_keeps_earliest(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    """Adding a later confirmation should not overwrite earlier first_confirmed_* values."""
    item_hash = fixture_message.item_hash

    early_tx = ChainTxDb(
        hash="0xearly",
        chain=Chain.ETH,
        height=500,
        datetime=pytz.utc.localize(dt.datetime(2022, 9, 1)),
        publisher="0xpub1",
        protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
        protocol_version=1,
        content="early",
    )
    late_tx = ChainTxDb(
        hash="0xlate",
        chain=Chain.ETH,
        height=900,
        datetime=pytz.utc.localize(dt.datetime(2022, 11, 1)),
        publisher="0xpub2",
        protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
        protocol_version=1,
        content="late",
    )

    with session_factory() as session:
        session.add(fixture_message)
        session.add(early_tx)
        session.add(late_tx)
        session.commit()

    # Insert LATER confirmation first
    with session_factory() as session:
        session.execute(
            make_confirmation_upsert_query(item_hash=item_hash, tx_hash=late_tx.hash)
        )
        session.commit()

        msg = get_message_by_item_hash(session=session, item_hash=ItemHash(item_hash))
        assert msg is not None
        assert msg.first_confirmed_at == late_tx.datetime
        assert msg.first_confirmed_height == late_tx.height

    # Now insert EARLIER confirmation — should replace
    with session_factory() as session:
        session.execute(
            make_confirmation_upsert_query(item_hash=item_hash, tx_hash=early_tx.hash)
        )
        session.commit()

        msg = get_message_by_item_hash(session=session, item_hash=ItemHash(item_hash))
        assert msg is not None
        assert msg.first_confirmed_at == early_tx.datetime
        assert msg.first_confirmed_height == early_tx.height


@pytest.mark.asyncio
async def test_upsert_query_message(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    message = copy(fixture_message)
    message.time = fixture_message.time - dt.timedelta(seconds=1)

    upsert_stmt = make_message_upsert_query(message)

    with session_factory() as session:
        session.add(message)
        session.commit()

    with session_factory() as session:
        session.execute(upsert_stmt)
        session.commit()

        message_db = get_message_by_item_hash(
            session=session, item_hash=ItemHash(message.item_hash)
        )

    assert message_db
    assert message_db.time == message.time


@pytest.mark.asyncio
async def test_get_unconfirmed_messages(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    with session_factory() as session:
        session.add(fixture_message)
        session.add(
            MessageStatusDb(
                item_hash=fixture_message.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=fixture_message.time,
            )
        )
        session.commit()

    with session_factory() as session:
        unconfirmed_messages = list(get_unconfirmed_messages(session))

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
    with session_factory() as session:
        session.add(tx)
        session.flush()
        session.execute(
            insert(message_confirmations).values(
                item_hash=fixture_message.item_hash, tx_hash=tx.hash
            )
        )
        session.commit()

    with session_factory() as session:
        # Check that the message is now ignored
        unconfirmed_messages = list(get_unconfirmed_messages(session))
        assert unconfirmed_messages == []

        # Check that the limit parameter is respected
        unconfirmed_messages = list(get_unconfirmed_messages(session, limit=0))
        assert unconfirmed_messages == []


@pytest.mark.asyncio
async def test_get_unconfirmed_messages_trusted_messages(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    fixture_message.signature = None
    with session_factory() as session:
        session.add(fixture_message)
        session.commit()

    with session_factory() as session:
        unconfirmed_messages = list(get_unconfirmed_messages(session))
        assert unconfirmed_messages == []


@pytest.mark.asyncio
async def test_get_distinct_channels(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    # TODO: improve this test
    #       * use several messages
    #       * test if None if considered as a channel
    #       * test
    with session_factory() as session:
        session.add(fixture_message)
        session.commit()
        channels = list(get_distinct_channels(session=session))

    assert channels == [fixture_message.channel]


@pytest.mark.asyncio
async def test_forget_message(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    with session_factory() as session:
        session.add(fixture_message)
        session.add(
            MessageStatusDb(
                item_hash=fixture_message.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=fixture_message.time,
            )
        )
        session.commit()

    forget_message_hash = (
        "d06251c954d4c75476c749e80b8f2a4962d20282b28b3e237e30b0a76157df2d"
    )
    forgotten_at = dt.datetime(2023, 5, 1, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        forget_message(
            session=session,
            item_hash=fixture_message.item_hash,
            forget_message_hash=forget_message_hash,
            forgotten_at=forgotten_at,
        )
        session.commit()

        message_status = get_message_status(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )
        assert message_status
        assert message_status.status == MessageStatus.FORGOTTEN

        # Assert that the message was deleted
        message = get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )
        assert message is None

        # Assert that the metadata was inserted properly in forgotten_messages
        forgotten_message = get_forgotten_message(
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

        # Billing metadata preserved at forget time
        assert forgotten_message.owner == fixture_message.owner
        # No payment field on the message: absence means hold
        assert forgotten_message.payment_type == "hold"
        # No file linked to this (aggregate) message
        assert forgotten_message.size is None
        assert forgotten_message.forgotten_at == forgotten_at

        # Now, add a hash to forgotten_by
        new_forget_message_hash = (
            "2aa1f44199181110e0c6b79ccc5e40ceaf20eac791dcfcd1b4f8f2f32b2d8502"
        )

        append_to_forgotten_by(
            session=session,
            forgotten_message_hash=fixture_message.item_hash,
            forget_message_hash=new_forget_message_hash,
        )
        session.commit()

        forgotten_message = get_forgotten_message(
            session=session, item_hash=fixture_message.item_hash
        )
        assert forgotten_message
        assert forgotten_message.forgotten_by == [
            forget_message_hash,
            new_forget_message_hash,
        ]


@pytest.mark.asyncio
async def test_forget_message_with_confirmations(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    """Forgetting a message that has confirmations should cascade-delete them."""

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
    ]
    fixture_message.confirmations = confirmations

    with session_factory() as session:
        session.add(fixture_message)
        session.add(
            MessageStatusDb(
                item_hash=fixture_message.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=fixture_message.time,
            )
        )
        session.commit()

    forget_message_hash = (
        "d06251c954d4c75476c749e80b8f2a4962d20282b28b3e237e30b0a76157df2d"
    )

    with session_factory() as session:
        # Verify the confirmation exists before forgetting
        rows = session.execute(
            select(message_confirmations).where(
                message_confirmations.c.item_hash == fixture_message.item_hash
            )
        ).all()
        assert len(rows) == 1

        forget_message(
            session=session,
            item_hash=fixture_message.item_hash,
            forget_message_hash=forget_message_hash,
            forgotten_at=dt.datetime(2023, 5, 1, tzinfo=dt.timezone.utc),
        )
        session.commit()

        # Message should be gone
        message = get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )
        assert message is None

        # Confirmations should be gone too
        rows = session.execute(
            select(message_confirmations).where(
                message_confirmations.c.item_hash == fixture_message.item_hash
            )
        ).all()
        assert len(rows) == 0

        # Forgotten record should exist
        forgotten = get_forgotten_message(
            session=session, item_hash=ItemHash(fixture_message.item_hash)
        )
        assert forgotten is not None
        assert forgotten.forgotten_by == [forget_message_hash]


# Real STORE message from the aleph.im API with payment.type = credit.
STORE_CREDIT_MESSAGE = {
    "sender": "0xB6B5358493AF8159B17506C5cC85df69193444BC",
    "chain": "ETH",
    "signature": "0x54c5168ad59ccc4da6b6bae82b0c59b3d2d7d0ce1bbae6081382a97e9fc8f39a356c701529704c4d9d4a13d9bde1f6d3b7a4c9520e0957078430b9e5d1ab95ef1b",
    "type": "STORE",
    "item_content": '{"address":"0xB6B5358493AF8159B17506C5cC85df69193444BC","item_type":"ipfs","item_hash":"QmePTEmasKHQQYdK3maUhrMJ7nxftSTFKeAGP7JweeiNrf","time":1771337941.575,"payment":{"chain":"ETH","type":"credit"}}',
    "item_type": "inline",
    "item_hash": "b81dcc3aa4827c693bc65d8ca1041387960cb4f4323e8be1984b604748ff02a8",
    "time": 1771337941.575,
    "channel": "ALEPH-CLOUDSOLUTIONS",
    "size": 189,
    "content": {
        "address": "0xB6B5358493AF8159B17506C5cC85df69193444BC",
        "time": 1771337941.575,
        "item_type": "ipfs",
        "item_hash": "QmePTEmasKHQQYdK3maUhrMJ7nxftSTFKeAGP7JweeiNrf",
        "payment": {"chain": "ETH", "type": "credit"},
    },
}


@pytest.mark.asyncio
async def test_payment_type_persisted_after_upsert(
    session_factory: DbSessionFactory,
):
    """Check that the denormalized payment_type column is populated in DB as expected."""
    message = make_validated_message_from_dict(STORE_CREDIT_MESSAGE)
    upsert_stmt = make_message_upsert_query(message)

    with session_factory() as session:
        session.execute(upsert_stmt)
        session.commit()

    with session_factory() as session:
        fetched = get_message_by_item_hash(
            session=session, item_hash=ItemHash(message.item_hash)
        )
        assert fetched is not None
        assert fetched.payment_type == "credit"


@pytest.mark.asyncio
async def test_forget_store_credit_message_preserves_billing_metadata(
    session_factory: DbSessionFactory,
):
    """Forgetting a credit-paid STORE preserves owner/payment_type/size/forgotten_at."""
    message = make_validated_message_from_dict(STORE_CREDIT_MESSAGE)
    store_content: Dict[str, Any] = STORE_CREDIT_MESSAGE["content"]  # type: ignore[assignment]
    file_hash = store_content["item_hash"]
    file_size = 5 * 1024 * 1024

    with session_factory() as session:
        session.execute(make_message_upsert_query(message))
        session.add(
            MessageStatusDb(
                item_hash=message.item_hash,
                status=MessageStatus.PROCESSED,
                reception_time=message.time,
            )
        )
        session.add(StoredFileDb(hash=file_hash, size=file_size, type=FileType.FILE))
        session.commit()

    forget_message_hash = (
        "0223e74dbae53b45da6a443fa18fd2a25f88677c82ed2de93f17ab24f78f58cf"
    )
    forgotten_at = dt.datetime(2026, 2, 20, 12, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        forget_message(
            session=session,
            item_hash=message.item_hash,
            forget_message_hash=forget_message_hash,
            forgotten_at=forgotten_at,
        )
        session.commit()

        forgotten_message = get_forgotten_message(
            session=session, item_hash=ItemHash(message.item_hash)
        )
        assert forgotten_message
        assert forgotten_message.owner == store_content["address"]
        assert forgotten_message.payment_type == "credit"
        assert forgotten_message.size == file_size
        assert forgotten_message.forgotten_at == forgotten_at


@pytest.mark.asyncio
async def test_migration_backfill_forgotten_at(
    session_factory: DbSessionFactory, fixture_message: MessageDb
):
    """
    Check the semantics of the 0061 migration backfill: forgotten_at is set
    from the time of the first forgetting FORGET message still present in
    the messages table.
    """
    forget_message_hash = fixture_message.item_hash
    forgotten_hash = "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe"

    with session_factory() as session:
        # The FORGET message (any message with a known hash/time works here)
        session.add(fixture_message)
        # A legacy forgotten_messages row without forgotten_at
        session.add(
            ForgottenMessageDb(
                item_hash=forgotten_hash,
                type=MessageType.store,
                chain=Chain.ETH,
                sender="0x51A58800b26AA1451aaA803d1746687cB88E0500",
                signature="sig",
                item_type=ItemType.inline,
                time=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
                channel=Channel("TEST"),
                forgotten_by=[forget_message_hash],
            )
        )
        session.commit()

        # Same statement as migration 0061
        session.execute(
            text(
                """
                UPDATE forgotten_messages fm
                SET forgotten_at = m.time
                FROM messages m
                WHERE m.item_hash = fm.forgotten_by[1]
                  AND fm.forgotten_at IS NULL
                """
            )
        )
        session.commit()

        forgotten_message = get_forgotten_message(
            session=session, item_hash=forgotten_hash
        )
        assert forgotten_message
        assert forgotten_message.forgotten_at == fixture_message.time


@pytest.mark.asyncio
async def test_migration_0061_column_additions_are_rerunnable(
    session_factory: DbSessionFactory,
):
    """
    Check the rerunnability of the 0061 migration column additions: the
    CONCURRENT index step commits the ALTER TABLE statements before alembic
    stamps the revision, so a rerun after an index failure re-executes them
    against a table that already has the columns and must not error.
    """
    with session_factory() as session:
        # Same statement as migration 0061, run against a schema where the
        # columns already exist (the test schema is created from the models).
        session.execute(
            text(
                """
                ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS owner VARCHAR;
                ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS payment_type VARCHAR;
                ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS size BIGINT;
                ALTER TABLE forgotten_messages ADD COLUMN IF NOT EXISTS forgotten_at TIMESTAMPTZ;
                """
            )
        )
        session.commit()


@pytest.mark.asyncio
async def test_migration_0063_moves_removed_messages(
    session_factory: DbSessionFactory,
):
    """
    Check the semantics of the 0063 migration data move: existing REMOVED
    messages are copied into removed_messages (size best-effort from a
    still-existing files row, removed_at unknown -> NULL) and their messages
    rows are deleted.
    """
    removed_hash = "beefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef"
    file_hash = "feedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed"
    file_size = 7 * 1024 * 1024

    with session_factory() as session:
        session.add(
            MessageDb(
                item_hash=removed_hash,
                sender="0x51A58800b26AA1451aaA803d1746687cB88E0500",
                chain=Chain.ETH,
                type=MessageType.store,
                time=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
                item_type=ItemType.inline,
                signature="sig",
                size=1000,
                content={
                    "address": "0x51A58800b26AA1451aaA803d1746687cB88E0500",
                    "time": 1640995200.0,
                    "item_hash": file_hash,
                    "item_type": "storage",
                },
                status_value=MessageStatus.REMOVED,
            )
        )
        session.add(
            MessageStatusDb(
                item_hash=removed_hash,
                status=MessageStatus.REMOVED,
                reception_time=dt.datetime(2022, 1, 2, tzinfo=dt.timezone.utc),
            )
        )
        session.add(StoredFileDb(hash=file_hash, size=file_size, type=FileType.FILE))
        session.commit()

        # Same statements as migration 0063
        session.execute(
            text(
                """
                INSERT INTO removed_messages (
                    item_hash, type, chain, sender, signature, item_type, time,
                    channel, owner, payment_type, size, removed_at
                )
                SELECT
                    m.item_hash, m.type, m.chain, m.sender, m.signature,
                    m.item_type, m.time, m.channel, m.owner,
                    COALESCE(m.payment_type, 'hold'),
                    f.size,
                    NULL
                FROM messages m
                JOIN message_status ms ON ms.item_hash = m.item_hash
                LEFT JOIN files f ON f.hash = m.content_item_hash
                WHERE ms.status = 'removed'
                ON CONFLICT (item_hash) DO UPDATE SET
                    type = EXCLUDED.type,
                    chain = EXCLUDED.chain,
                    sender = EXCLUDED.sender,
                    signature = EXCLUDED.signature,
                    item_type = EXCLUDED.item_type,
                    time = EXCLUDED.time,
                    channel = EXCLUDED.channel,
                    owner = EXCLUDED.owner,
                    payment_type = EXCLUDED.payment_type,
                    size = COALESCE(removed_messages.size, EXCLUDED.size)
                """
            )
        )
        session.execute(
            text(
                """
                DELETE FROM messages m
                USING message_status ms
                WHERE ms.item_hash = m.item_hash
                  AND ms.status = 'removed'
                """
            )
        )
        session.commit()

        snapshot = session.get(RemovedMessageDb, removed_hash)
        assert snapshot is not None
        assert snapshot.type == MessageType.store
        assert snapshot.sender == "0x51A58800b26AA1451aaA803d1746687cB88E0500"
        # content.address is denormalized into messages.owner at insert time
        assert snapshot.owner == "0x51A58800b26AA1451aaA803d1746687cB88E0500"
        # No payment field: absence means hold
        assert snapshot.payment_type == "hold"
        # Size backfilled from the still-existing files row
        assert snapshot.size == file_size
        # Removal time unknown for legacy rows
        assert snapshot.removed_at is None

        assert session.get(MessageDb, removed_hash) is None
