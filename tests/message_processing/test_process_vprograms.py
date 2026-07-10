import copy
import datetime as dt
import json

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from message_test_helpers import process_pending_messages
from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH
from more_itertools import one
from sqlalchemy import func, select

from aleph.db.accessors.cost import get_message_costs
from aleph.db.accessors.messages import (
    get_message_by_item_hash,
    get_message_status,
    get_rejected_message,
)
from aleph.db.accessors.vms import get_instance, get_program, get_vprogram
from aleph.db.models import (
    AlephCreditBalanceDb,
    MessageStatusDb,
    PendingMessageDb,
    VProgramVerifiedVolumeDb,
)
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.schemas.api.messages import format_message
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_processing_result import ProcessedMessage, RejectedMessage
from aleph.types.message_status import ErrorCode, MessageStatus
from aleph.types.vms import VmType

SENDER = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba"


@pytest.fixture
def fixture_vprogram_message(session_factory: DbSessionFactory) -> PendingMessageDb:
    pending_message = PendingMessageDb(
        item_hash=VPROGRAM_ITEM_HASH,
        type=MessageType.v_program,
        chain=Chain.ETH,
        sender=SENDER,
        signature=None,
        item_type=ItemType.inline,
        item_content=json.dumps(VPROGRAM_CONTENT),
        time=timestamp_to_datetime(1719502000.0),
        channel=None,
        reception_time=timestamp_to_datetime(1719502001),
        fetched=True,
        check_message=False,
        retries=0,
        next_attempt=dt.datetime(2026, 1, 1),
    )
    with session_factory() as session:
        session.add(pending_message)
        session.add(
            MessageStatusDb(
                item_hash=pending_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=pending_message.reception_time,
            )
        )
        session.commit()
    return pending_message


@pytest.fixture
def user_credit_balance(session_factory: DbSessionFactory) -> None:
    with session_factory() as session:
        session.add(
            AlephCreditBalanceDb(
                address=SENDER,
                credit_ref="test-credit-ref",
                credit_index=0,
                amount_remaining=1_000_000_000,
                expiration_date=None,
                message_timestamp=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.commit()


@pytest.mark.asyncio
async def test_process_vprogram(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    with session_factory() as session:
        status = get_message_status(
            session=session, item_hash=ItemHash(fixture_vprogram_message.item_hash)
        )
        assert status is not None
        assert status.status == MessageStatus.PROCESSED

        # Costs were persisted for the credit payment.
        costs = list(
            get_message_costs(
                session=session, item_hash=fixture_vprogram_message.item_hash
            )
        )
        assert costs
        assert all(cost.owner == SENDER for cost in costs)

        # The vms representation was written, under its own polymorphic
        # identity (neither an instance nor a program).
        vprogram = get_vprogram(
            session=session, item_hash=fixture_vprogram_message.item_hash
        )
        assert vprogram is not None
        assert vprogram.type == VmType.VPROGRAM
        assert vprogram.owner == SENDER
        assert vprogram.payment_type == "credit"
        assert vprogram.environment_internet is True
        assert vprogram.runtime_ref == VPROGRAM_CONTENT["runtime"]["ref"]
        assert vprogram.runtime_comment == VPROGRAM_CONTENT["runtime"]["comment"]
        assert vprogram.workload_ref == VPROGRAM_CONTENT["workload"]["ref"]
        assert vprogram.workload_hash_tree == VPROGRAM_CONTENT["workload"]["hash_tree"]
        assert vprogram.workload_roothash == VPROGRAM_CONTENT["workload"]["roothash"]

        volume_content = VPROGRAM_CONTENT["volumes"][0]
        assert len(vprogram.verified_volumes) == 1
        volume = vprogram.verified_volumes[0]
        assert volume.position == 0
        assert volume.ref == volume_content["ref"]
        assert volume.hash_tree == volume_content["hash_tree"]
        assert volume.roothash == volume_content["roothash"]
        assert volume.comment == volume_content["comment"]

        assert (
            get_instance(session=session, item_hash=fixture_vprogram_message.item_hash)
            is None
        )
        assert (
            get_program(session=session, item_hash=fixture_vprogram_message.item_hash)
            is None
        )

        # The stored message serializes through the API model.
        message = get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_vprogram_message.item_hash)
        )
        assert message is not None
        formatted = format_message(message)
        assert formatted.type == MessageType.v_program


@pytest.mark.asyncio
async def test_process_vprogram_insufficient_credit(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # No credit balance is seeded: processing must reject the message.
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    with session_factory() as session:
        status = get_message_status(
            session=session, item_hash=ItemHash(fixture_vprogram_message.item_hash)
        )
        assert status is not None
        assert status.status == MessageStatus.REJECTED

        rejected = get_rejected_message(
            session=session, item_hash=fixture_vprogram_message.item_hash
        )
        assert rejected is not None
        assert rejected.error_code == ErrorCode.CREDIT_INSUFFICIENT


def _pending_message(
    item_hash: str,
    message_type: MessageType,
    content: dict,
    time: float,
) -> PendingMessageDb:
    return PendingMessageDb(
        item_hash=item_hash,
        type=message_type,
        chain=Chain.ETH,
        sender=SENDER,
        signature=None,
        item_type=ItemType.inline,
        item_content=json.dumps(content),
        time=timestamp_to_datetime(time),
        channel=None,
        reception_time=timestamp_to_datetime(time + 1),
        fetched=True,
        check_message=False,
        retries=0,
        next_attempt=dt.datetime(2026, 1, 1),
    )


@pytest.mark.asyncio
async def test_forget_store_used_by_vprogram_is_blocked(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Forgetting a STORE file referenced by a live V-Program (here: the
    workload image) must be blocked; forgetting the V-Program itself must
    delete its vms rows, after which the STORE becomes forgettable."""

    file_hash = "f0" * 32
    store_message_hash = "50" * 32
    vprogram_message_hash = "51" * 32

    store_message = _pending_message(
        item_hash=store_message_hash,
        message_type=MessageType.store,
        content={
            "address": SENDER,
            "time": 1719502000.0,
            "item_type": "storage",
            "item_hash": file_hash,
            "mime_type": "text/plain",
        },
        time=1719502000.0,
    )

    vprogram_content = copy.deepcopy(VPROGRAM_CONTENT)
    vprogram_content["workload"]["ref"] = store_message_hash
    vprogram_message = _pending_message(
        item_hash=vprogram_message_hash,
        message_type=MessageType.v_program,
        content=vprogram_content,
        time=1719502010.0,
    )

    def forget_message(item_hash: str, target: str, time: float) -> PendingMessageDb:
        return _pending_message(
            item_hash=item_hash,
            message_type=MessageType.forget,
            content={"address": SENDER, "time": time, "hashes": [target]},
            time=time,
        )

    storage_engine = message_processor.message_handler.storage_service.storage_engine
    await storage_engine.write(filename=file_hash, content=b"workload image")

    with session_factory() as session:
        store_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[store_message],
                session=session,
            )
        )
        assert isinstance(store_result, ProcessedMessage)

        vprogram_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[vprogram_message],
                session=session,
            )
        )
        assert isinstance(vprogram_result, ProcessedMessage)
        assert (
            get_vprogram(session=session, item_hash=vprogram_message_hash) is not None
        )

        # Forgetting the STORE while the V-Program references it is blocked.
        blocked_forget_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[
                    forget_message("52" * 32, store_message_hash, 1719502020.0)
                ],
                session=session,
            )
        )
        assert isinstance(blocked_forget_result, RejectedMessage)
        rejected = get_rejected_message(session=session, item_hash="52" * 32)
        assert rejected is not None
        assert rejected.error_code == ErrorCode.FORGET_NOT_ALLOWED

        store_status = get_message_status(
            session=session, item_hash=ItemHash(store_message_hash)
        )
        assert store_status is not None
        assert store_status.status == MessageStatus.PROCESSED

        # Forgetting the V-Program deletes its vms representation...
        vprogram_forget_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[
                    forget_message("53" * 32, vprogram_message_hash, 1719502030.0)
                ],
                session=session,
            )
        )
        assert isinstance(vprogram_forget_result, ProcessedMessage)
        assert get_vprogram(session=session, item_hash=vprogram_message_hash) is None
        remaining_volumes = session.execute(
            select(func.count()).select_from(VProgramVerifiedVolumeDb)
        ).scalar_one()
        assert remaining_volumes == 0

        # ... after which the STORE can be forgotten.
        store_forget_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[
                    forget_message("54" * 32, store_message_hash, 1719502040.0)
                ],
                session=session,
            )
        )
        assert isinstance(store_forget_result, ProcessedMessage)
        # The pipeline commits in its own sessions: expire this session's
        # identity map so the status re-read hits the database.
        session.expire_all()
        store_status = get_message_status(
            session=session, item_hash=ItemHash(store_message_hash)
        )
        assert store_status is not None
        assert store_status.status == MessageStatus.FORGOTTEN
