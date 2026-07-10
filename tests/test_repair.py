import datetime as dt

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from sqlalchemy import func, select

from aleph.db.accessors.messages import get_message_status, get_rejected_message
from aleph.db.accessors.vms import get_vms_dependent_volumes, get_vprogram
from aleph.db.models import (
    MessageDb,
    MessageStatusDb,
    VProgramDb,
    VProgramVerifiedVolumeDb,
)
from aleph.repair import (
    _reject_invalid_program_metadata,
    mark_processed_message_as_rejected,
)
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


def test_mark_processed_vprogram_as_rejected_deletes_vms_rows(
    session_factory: DbSessionFactory,
):
    """Rejecting a processed V-PROGRAM must delete its vms representation:
    the vms rows have no FK to messages, and an orphaned row would keep
    blocking forgets of the files it references."""
    vprogram_hash = "feed" + "0" * 60
    workload_ref = "3c" * 32

    message = MessageDb(
        item_hash=vprogram_hash,
        chain=Chain.ETH,
        sender="0x0000000000000000000000000000000000000001",
        signature="0xsig",
        item_type=ItemType.inline,
        type=MessageType.v_program,
        content={"address": "0xabc", "time": 1700000000.0},
        size=100,
        time=timestamp_to_datetime(1700000000.0),
        channel=Channel("TEST"),
    )
    vprogram_row = VProgramDb(
        item_hash=vprogram_hash,
        owner="0xabc",
        allow_amend=False,
        environment_reproducible=False,
        environment_internet=True,
        environment_aleph_api=False,
        environment_shared_cache=False,
        resources_vcpus=2,
        resources_memory=2048,
        resources_seconds=30,
        created=timestamp_to_datetime(1700000000.0),
        runtime_ref="2b" * 32,
        runtime_comment="",
        workload_ref=workload_ref,
        workload_hash_tree="4d" * 32,
        workload_roothash="ab" * 32,
        verified_volumes=[
            VProgramVerifiedVolumeDb(
                position=0,
                ref="5e" * 32,
                hash_tree="6f" * 32,
                roothash="cd" * 32,
                comment="",
            )
        ],
    )

    with session_factory() as session:
        _seed(session, message)
        session.add(vprogram_row)
        session.commit()

    with session_factory() as session:
        message_db = session.execute(
            select(MessageDb).where(MessageDb.item_hash == vprogram_hash)
        ).scalar_one()
        mark_processed_message_as_rejected(
            session=session,
            message=message_db,
            error_code=ErrorCode.INVALID_FORMAT,
            reason="test rejection",
        )
        session.commit()

    with session_factory() as session:
        status = get_message_status(session=session, item_hash=ItemHash(vprogram_hash))
        assert status is not None
        assert status.status == MessageStatus.REJECTED

        assert get_vprogram(session=session, item_hash=vprogram_hash) is None
        remaining_volumes = session.execute(
            select(func.count()).select_from(VProgramVerifiedVolumeDb)
        ).scalar_one()
        assert remaining_volumes == 0
        # The referenced files are forgettable again.
        assert (
            get_vms_dependent_volumes(session=session, volume_hash=workload_ref) is None
        )
