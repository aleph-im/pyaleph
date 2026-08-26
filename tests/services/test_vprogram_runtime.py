import datetime as dt
import json

import pytest
from in_memory_storage_engine import InMemoryStorageEngine

from aleph.db.accessors.files import insert_message_file_pin
from aleph.db.models import StoredFileDb
from aleph.services.vprogram_runtime import (
    resolve_runtime_bundle_ref,
    runtime_bundle_volume,
)
from aleph.storage import StorageService
from aleph.types.cost import CostType
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import ErrorCode, InvalidVProgramRuntime

OWNER = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba"
MANIFEST_REF = "cafe" * 16
MANIFEST_FILE_HASH = MANIFEST_REF[::-1]
BUNDLE_REF = "ba" * 32

MANIFEST = {
    "format": "aleph-vprogram-runtime",
    "format_version": 1,
    "name": "compose-runner",
    "version": "0.1.0",
    "platform": "sev_snp",
    "bundle": {
        "ref": BUNDLE_REF,
        # Deliberately absurd: pricing must never read this number.
        "size": 1,
        "sha256": "00" * 32,
        "members": {},
    },
    "boot": {},
    "attestation": [],
    "workload": {},
    "source": {},
}


def pin_manifest(session: DbSession) -> None:
    session.add(StoredFileDb(hash=MANIFEST_FILE_HASH, size=512, type=FileType.FILE))
    session.flush()
    insert_message_file_pin(
        session=session,
        file_hash=MANIFEST_FILE_HASH,
        owner=OWNER,
        item_hash=MANIFEST_REF,
        ref=None,
        created=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
    )


def make_storage_service(mocker, files: dict) -> StorageService:
    return StorageService(
        storage_engine=InMemoryStorageEngine(files=files),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )


@pytest.mark.asyncio
async def test_resolve_bundle_ref_from_pinned_manifest(
    session_factory: DbSessionFactory, mocker
):
    storage_service = make_storage_service(
        mocker, {MANIFEST_FILE_HASH: json.dumps(MANIFEST).encode()}
    )
    with session_factory() as session:
        pin_manifest(session)
        session.commit()
        bundle_ref = await resolve_runtime_bundle_ref(
            session, storage_service, MANIFEST_REF
        )
    assert bundle_ref == BUNDLE_REF


@pytest.mark.asyncio
async def test_resolve_bundle_ref_rejects_unpinned_manifest(
    session_factory: DbSessionFactory, mocker
):
    storage_service = make_storage_service(mocker, {})
    with session_factory() as session:
        with pytest.raises(InvalidVProgramRuntime) as exc_info:
            await resolve_runtime_bundle_ref(session, storage_service, MANIFEST_REF)
    assert exc_info.value.error_code == ErrorCode.VM_RUNTIME_INVALID


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        json.dumps(
            {"format": "something-else", "bundle": {"ref": BUNDLE_REF}}
        ).encode(),
        json.dumps({"format": "aleph-vprogram-runtime", "bundle": {}}).encode(),
        json.dumps(
            {"format": "aleph-vprogram-runtime", "bundle": {"ref": "not-a-hash"}}
        ).encode(),
    ],
)
async def test_resolve_bundle_ref_rejects_invalid_manifest(
    session_factory: DbSessionFactory, mocker, raw: bytes
):
    storage_service = make_storage_service(mocker, {MANIFEST_FILE_HASH: raw})
    with session_factory() as session:
        pin_manifest(session)
        session.commit()
        with pytest.raises(InvalidVProgramRuntime):
            await resolve_runtime_bundle_ref(session, storage_service, MANIFEST_REF)


def test_runtime_bundle_volume_shape():
    volume = runtime_bundle_volume(BUNDLE_REF)
    assert volume.cost_type == CostType.EXECUTION_VPROGRAM_VOLUME
    assert volume.ref == BUNDLE_REF
    assert volume.use_latest is False
    assert volume.name == "runtime"
