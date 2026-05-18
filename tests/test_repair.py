import datetime as dt

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from sqlalchemy import select

from aleph.db.accessors.messages import get_message_status, get_rejected_message
from aleph.db.models import MessageDb, MessageStatusDb
from aleph.repair import _reject_invalid_program_metadata
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import ErrorCode, MessageStatus


def _program_message(item_hash: str, content: dict) -> MessageDb:
    return MessageDb(
        item_hash=item_hash,
        chain=Chain.ETH,
        sender="0x0000000000000000000000000000000000000001",
        signature="0xsig",
        item_type=ItemType.inline,
        type=MessageType.program,
        content=content,
        size=100,
        time=timestamp_to_datetime(1700000000.0),
        channel=Channel("TEST"),
    )


def _seed(session, message: MessageDb) -> None:
    session.add(message)
    session.add(
        MessageStatusDb(
            item_hash=message.item_hash,
            status=MessageStatus.PROCESSED,
            reception_time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        )
    )


def test_reject_invalid_program_metadata_rejects_list_metadata(
    session_factory: DbSessionFactory,
):
    """PROGRAM rows whose content.metadata is a JSON array should be moved to
    REJECTED so the API stops 500ing on them."""
    bad_hash = "bad" + "0" * 61
    good_hash = "dad" + "0" * 61

    with session_factory() as session:
        _seed(
            session,
            _program_message(
                bad_hash,
                {"address": "0xabc", "metadata": ["legacy-list-value"]},
            ),
        )
        _seed(
            session,
            _program_message(
                good_hash,
                {"address": "0xdef", "metadata": {"name": "good"}},
            ),
        )
        session.commit()

    _reject_invalid_program_metadata(session_factory)

    with session_factory() as session:
        # Bad message: gone from `messages`, present in `rejected_messages`,
        # status flipped to REJECTED.
        assert (
            session.execute(
                select(MessageDb).where(MessageDb.item_hash == bad_hash)
            ).scalar_one_or_none()
            is None
        )

        rejected = get_rejected_message(session=session, item_hash=bad_hash)
        assert rejected is not None
        assert rejected.error_code == ErrorCode.INVALID_FORMAT
        assert rejected.message["item_hash"] == bad_hash
        assert rejected.message["content"]["metadata"] == ["legacy-list-value"]

        status = get_message_status(session=session, item_hash=ItemHash(bad_hash))
        assert status is not None
        assert status.status == MessageStatus.REJECTED

        # Good message: untouched.
        good = session.execute(
            select(MessageDb).where(MessageDb.item_hash == good_hash)
        ).scalar_one_or_none()
        assert good is not None
        assert good.status_value == MessageStatus.PROCESSED

        good_status = get_message_status(session=session, item_hash=ItemHash(good_hash))
        assert good_status is not None
        assert good_status.status == MessageStatus.PROCESSED

        assert get_rejected_message(session=session, item_hash=good_hash) is None


def test_reject_invalid_program_metadata_ignores_other_types(
    session_factory: DbSessionFactory,
):
    """The repair targets PROGRAM only. Other message types stay put even if
    they happen to carry a `metadata` array (the schema differs)."""
    aggregate_hash = "agg" + "0" * 61

    with session_factory() as session:
        message = MessageDb(
            item_hash=aggregate_hash,
            chain=Chain.ETH,
            sender="0x0000000000000000000000000000000000000002",
            signature="0xsig",
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            content={"address": "0xabc", "metadata": ["whatever"]},
            size=100,
            time=timestamp_to_datetime(1700000000.0),
            channel=Channel("TEST"),
        )
        session.add(message)
        session.add(
            MessageStatusDb(
                item_hash=aggregate_hash,
                status=MessageStatus.PROCESSED,
                reception_time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.commit()

    _reject_invalid_program_metadata(session_factory)

    with session_factory() as session:
        msg = session.execute(
            select(MessageDb).where(MessageDb.item_hash == aggregate_hash)
        ).scalar_one_or_none()
        assert msg is not None
        assert get_rejected_message(session=session, item_hash=aggregate_hash) is None


def test_reject_invalid_program_metadata_no_op_when_empty(
    session_factory: DbSessionFactory,
):
    """Empty DB should be a no-op rather than an error."""
    _reject_invalid_program_metadata(session_factory)


@pytest.mark.parametrize(
    "metadata_value",
    [
        {"name": "valid"},
        None,
    ],
)
def test_reject_invalid_program_metadata_skips_valid_programs(
    session_factory: DbSessionFactory, metadata_value
):
    """Programs with a dict metadata (or none at all) should not be touched."""
    item_hash = "ok" + "0" * 62

    content: dict = {"address": "0xabc"}
    if metadata_value is not None:
        content["metadata"] = metadata_value

    with session_factory() as session:
        _seed(session, _program_message(item_hash, content))
        session.commit()

    _reject_invalid_program_metadata(session_factory)

    with session_factory() as session:
        assert (
            session.execute(
                select(MessageDb).where(MessageDb.item_hash == item_hash)
            ).scalar_one_or_none()
            is not None
        )
        assert get_rejected_message(session=session, item_hash=item_hash) is None
