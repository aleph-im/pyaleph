import copy
import datetime as dt
import json
from decimal import Decimal
from typing import List

import pytest
from aleph_message.models import (
    Chain,
    ItemHash,
    ItemType,
    MessageType,
    VerifiableProgramContent,
)
from in_memory_storage_engine import InMemoryStorageEngine
from message_test_helpers import process_pending_messages
from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH
from more_itertools import one
from sqlalchemy import func, select

from aleph.db.accessors.cost import get_message_costs
from aleph.db.accessors.files import find_file_pins, insert_message_file_pin
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
    StoredFileDb,
    VProgramVerifiedVolumeDb,
)
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.schemas.api.messages import format_message
from aleph.services.vprogram_runtime import resolve_runtime_bundle_ref
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_processing_result import ProcessedMessage, RejectedMessage
from aleph.types.message_status import ErrorCode, MessageStatus
from aleph.types.vms import VmType

SENDER = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba"


def get_vprogram_store_refs(content: VerifiableProgramContent) -> List[str]:
    refs = [
        str(content.runtime.ref),
        str(content.workload.ref),
        str(content.workload.hash_tree),
    ]
    for volume in content.volumes:
        refs.append(str(volume.ref))
        refs.append(str(volume.hash_tree))
    return refs


def insert_vprogram_refs(session: DbSession, message: PendingMessageDb):
    """
    Insert the file pins referenced by the V-Program to make it processable.
    Refs that are already pinned (e.g. by a real STORE message) are skipped.
    """

    assert message.item_content
    content = VerifiableProgramContent.model_validate_json(message.item_content)
    refs = set(get_vprogram_store_refs(content))
    refs -= set(find_file_pins(session=session, item_hashes=refs))

    created = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)

    for ref in refs:
        # As in the program tests, the file hash just has to be a valid hash:
        # use the reversed ref.
        file_hash = ref[::-1]
        session.add(StoredFileDb(hash=file_hash, size=1024 * 1024, type=FileType.FILE))
        session.flush()
        insert_message_file_pin(
            session=session,
            file_hash=file_hash,
            owner=content.address,
            item_hash=ref,
            ref=None,
            created=created,
        )


BUNDLE_REF = "ba" * 32
MANIFEST_FILE_HASH = VPROGRAM_CONTENT["runtime"]["ref"][::-1]
MANIFEST = {
    "format": "aleph-vprogram-runtime",
    "format_version": 1,
    "platform": "sev_snp",
    "bundle": {"ref": BUNDLE_REF, "size": 1, "sha256": "00" * 32},
}


def insert_bundle_pin(
    session: DbSession, size_bytes: int = 3 * 1024 * 1024 * 1024
) -> None:
    session.add(
        StoredFileDb(hash=BUNDLE_REF[::-1], size=size_bytes, type=FileType.FILE)
    )
    session.flush()
    insert_message_file_pin(
        session=session,
        file_hash=BUNDLE_REF[::-1],
        owner=SENDER,
        item_hash=BUNDLE_REF,
        ref=None,
        created=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
    )


def store_manifest(message_processor: PendingMessageProcessor, raw: bytes) -> None:
    """Put the manifest bytes where the handler's StorageService reads them."""
    storage_engine = message_processor.message_handler.storage_service.storage_engine
    assert isinstance(storage_engine, InMemoryStorageEngine)
    storage_engine.files[MANIFEST_FILE_HASH] = raw


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
    with session_factory() as session:
        insert_vprogram_refs(session, fixture_vprogram_message)
        insert_bundle_pin(session)
        session.commit()
    store_manifest(message_processor, json.dumps(MANIFEST).encode())

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

        artifact_rows = {
            c.name: c for c in costs if c.type == "EXECUTION_VPROGRAM_VOLUME"
        }
        assert set(artifact_rows) == {
            "workload",
            "workload:hash_tree",
            "#0:model weights",
            "#0:model weights:hash_tree",
            "runtime",
        }
        assert artifact_rows["runtime"].ref == BUNDLE_REF
        # 3 GiB bundle + 4 x 1 MiB artifacts is under the 2 CU x 20 GiB
        # allowance: the discount cancels the artifact rows exactly.
        discount = next(c for c in costs if c.type == "EXECUTION_VOLUME_DISCOUNT")
        assert Decimal(discount.cost_credit) == -sum(
            Decimal(c.cost_credit) for c in artifact_rows.values()
        )

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
        # The manifest's bundle ref is persisted, so recalculation never has
        # to re-read the manifest and the bundle STORE is forget-protected.
        assert vprogram.runtime_bundle_ref == BUNDLE_REF
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
async def test_process_vprogram_reads_manifest_once(
    mocker,
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """check_dependencies and check_balance both need the manifest's bundle
    ref: the handler must cache it instead of resolving (and re-reading the
    manifest STORE) once per hook for the same message."""
    with session_factory() as session:
        insert_vprogram_refs(session, fixture_vprogram_message)
        insert_bundle_pin(session)
        session.commit()
    store_manifest(message_processor, json.dumps(MANIFEST).encode())

    spy = mocker.patch(
        "aleph.handlers.content.vprogram.resolve_runtime_bundle_ref",
        wraps=resolve_runtime_bundle_ref,
    )

    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    with session_factory() as session:
        status = get_message_status(
            session=session, item_hash=ItemHash(fixture_vprogram_message.item_hash)
        )
        assert status is not None
        assert status.status == MessageStatus.PROCESSED

    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_process_vprogram_insufficient_credit(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # No credit balance is seeded: processing must reject the message.
    with session_factory() as session:
        insert_vprogram_refs(session, fixture_vprogram_message)
        insert_bundle_pin(session)
        session.commit()
    store_manifest(message_processor, json.dumps(MANIFEST).encode())

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


@pytest.mark.asyncio
async def test_process_vprogram_missing_refs(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """A V-PROGRAM whose store references (runtime manifest, workload image
    and hash tree, verified volumes and their hash trees) are unknown to the
    node must not be processed, mirroring programs with missing volumes."""

    # No refs are seeded: all five store references are missing.
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    with session_factory() as session:
        assert (
            get_vprogram(session=session, item_hash=fixture_vprogram_message.item_hash)
            is None
        )

        status = get_message_status(
            session=session, item_hash=ItemHash(fixture_vprogram_message.item_hash)
        )
        assert status is not None
        assert status.status == MessageStatus.REJECTED

        rejected = get_rejected_message(
            session=session, item_hash=fixture_vprogram_message.item_hash
        )
        assert rejected is not None
        assert rejected.error_code == ErrorCode.VM_VOLUME_NOT_FOUND

        assert fixture_vprogram_message.item_content
        content = VerifiableProgramContent.model_validate_json(
            fixture_vprogram_message.item_content
        )
        assert isinstance(rejected.details, dict)
        assert set(rejected.details["errors"]) == set(get_vprogram_store_refs(content))
        assert rejected.traceback is None


@pytest.mark.asyncio
async def test_process_vprogram_rejects_invalid_manifest(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    with session_factory() as session:
        insert_vprogram_refs(session, fixture_vprogram_message)
        session.commit()
    store_manifest(message_processor, b"{not a manifest")

    pipeline = message_processor.make_pipeline()
    results = [message async for batch in pipeline for message in batch]

    result = one(results)
    assert isinstance(result, RejectedMessage)
    assert result.error_code == ErrorCode.VM_RUNTIME_INVALID
    with session_factory() as session:
        rejected = get_rejected_message(session=session, item_hash=VPROGRAM_ITEM_HASH)
        assert rejected is not None
        assert rejected.error_code == ErrorCode.VM_RUNTIME_INVALID


@pytest.mark.asyncio
async def test_process_vprogram_unpinned_bundle_is_a_missing_volume(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    with session_factory() as session:
        insert_vprogram_refs(session, fixture_vprogram_message)
        session.commit()
    # Valid manifest, but its bundle is not pinned on this node.
    store_manifest(message_processor, json.dumps(MANIFEST).encode())

    pipeline = message_processor.make_pipeline()
    results = [message async for batch in pipeline for message in batch]

    result = one(results)
    assert isinstance(result, RejectedMessage)
    assert result.error_code == ErrorCode.VM_VOLUME_NOT_FOUND


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
    """Forgetting a STORE file referenced by a live V-Program (the workload
    image, or the runtime bundle the manifest names) must be blocked;
    forgetting the V-Program itself must delete its vms rows, after which
    the STOREs become forgettable."""

    file_hash = "f0" * 32
    store_message_hash = "50" * 32
    vprogram_message_hash = "51" * 32
    # The bundle is a STORE message like any other: it is never named by the
    # V-PROGRAM message, only by the runtime manifest it points at.
    bundle_file_hash = "f1" * 32
    bundle_store_hash = "55" * 32

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

    bundle_store_message = _pending_message(
        item_hash=bundle_store_hash,
        message_type=MessageType.store,
        content={
            "address": SENDER,
            "time": 1719502001.0,
            "item_type": "storage",
            "item_hash": bundle_file_hash,
            "mime_type": "application/octet-stream",
        },
        time=1719502001.0,
    )

    vprogram_content = copy.deepcopy(VPROGRAM_CONTENT)
    vprogram_content["workload"]["ref"] = store_message_hash
    vprogram_message = _pending_message(
        item_hash=vprogram_message_hash,
        message_type=MessageType.v_program,
        content=vprogram_content,
        time=1719502010.0,
    )

    bundle_manifest: dict = copy.deepcopy(MANIFEST)
    bundle_manifest["bundle"]["ref"] = bundle_store_hash

    def forget_message(item_hash: str, target: str, time: float) -> PendingMessageDb:
        return _pending_message(
            item_hash=item_hash,
            message_type=MessageType.forget,
            content={"address": SENDER, "time": time, "hashes": [target]},
            time=time,
        )

    storage_engine = message_processor.message_handler.storage_service.storage_engine
    await storage_engine.write(filename=file_hash, content=b"workload image")
    await storage_engine.write(filename=bundle_file_hash, content=b"runtime bundle")

    with session_factory() as session:
        store_results = await process_pending_messages(
            message_processor=message_processor,
            pending_messages=[store_message, bundle_store_message],
            session=session,
        )
        assert all(isinstance(result, ProcessedMessage) for result in store_results)

        # The workload ref and the bundle are pinned by the STORE messages
        # processed above; seed the other references (runtime, hash trees,
        # volume) so the dependency check passes.
        insert_vprogram_refs(session, vprogram_message)
        store_manifest(message_processor, json.dumps(bundle_manifest).encode())

        vprogram_result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[vprogram_message],
                session=session,
            )
        )
        assert isinstance(vprogram_result, ProcessedMessage)
        vprogram = get_vprogram(session=session, item_hash=vprogram_message_hash)
        assert vprogram is not None
        assert vprogram.runtime_bundle_ref == bundle_store_hash

        # Forgetting the workload STORE, or the bundle STORE the manifest
        # names, while the V-Program references them is blocked.
        for i, (forget_hash, target) in enumerate(
            [("52" * 32, store_message_hash), ("56" * 32, bundle_store_hash)]
        ):
            blocked_forget_result = one(
                await process_pending_messages(
                    message_processor=message_processor,
                    pending_messages=[
                        forget_message(forget_hash, target, 1719502020.0 + i)
                    ],
                    session=session,
                )
            )
            assert isinstance(blocked_forget_result, RejectedMessage)
            rejected = get_rejected_message(session=session, item_hash=forget_hash)
            assert rejected is not None
            assert rejected.error_code == ErrorCode.FORGET_NOT_ALLOWED

            target_status = get_message_status(
                session=session, item_hash=ItemHash(target)
            )
            assert target_status is not None
            assert target_status.status == MessageStatus.PROCESSED

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

        # ... after which both STOREs can be forgotten.
        for i, (forget_hash, target) in enumerate(
            [("54" * 32, store_message_hash), ("57" * 32, bundle_store_hash)]
        ):
            store_forget_result = one(
                await process_pending_messages(
                    message_processor=message_processor,
                    pending_messages=[
                        forget_message(forget_hash, target, 1719502040.0 + i)
                    ],
                    session=session,
                )
            )
            assert isinstance(store_forget_result, ProcessedMessage)
            # The pipeline commits in its own sessions: expire this session's
            # identity map so the status re-read hits the database.
            session.expire_all()
            target_status = get_message_status(
                session=session, item_hash=ItemHash(target)
            )
            assert target_status is not None
            assert target_status.status == MessageStatus.FORGOTTEN
