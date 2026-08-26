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


def pin_manifest(
    session: DbSession,
    item_hash: str = MANIFEST_REF,
    file_hash: str = MANIFEST_FILE_HASH,
) -> None:
    session.add(StoredFileDb(hash=file_hash, size=512, type=FileType.FILE))
    session.flush()
    insert_message_file_pin(
        session=session,
        file_hash=file_hash,
        owner=OWNER,
        item_hash=item_hash,
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
        b"",
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


@pytest.mark.asyncio
async def test_resolve_bundle_ref_rejects_unreadable_pinned_manifest(
    session_factory: DbSessionFactory, mocker
):
    """The manifest is pinned, but its bytes are nowhere: not in local
    storage, not reachable over P2P, not on IPFS. This exercises the
    `get_hash_content` failure branch, distinct from an altogether unpinned
    manifest."""
    unreadable_ref = "dead" * 16
    unreadable_file_hash = unreadable_ref[::-1]
    storage_service = make_storage_service(mocker, {})
    with session_factory() as session:
        pin_manifest(session, item_hash=unreadable_ref, file_hash=unreadable_file_hash)
        session.commit()
        with pytest.raises(InvalidVProgramRuntime):
            await resolve_runtime_bundle_ref(session, storage_service, unreadable_ref)


@pytest.mark.asyncio
async def test_resolve_bundle_ref_rejects_unreadable_manifest_pinned_on_ipfs(
    session_factory: DbSessionFactory, mocker
):
    """The manifest is pinned under an IPFS CID-shaped file hash, exercising
    the `item_type_from_hash` ipfs branch. The content is unreachable
    everywhere, including IPFS itself."""
    ipfs_ref = "beef" * 16
    ipfs_file_hash = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_content = mocker.AsyncMock(return_value=None)
    storage_service = StorageService(
        storage_engine=InMemoryStorageEngine(files={}),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    with session_factory() as session:
        pin_manifest(session, item_hash=ipfs_ref, file_hash=ipfs_file_hash)
        session.commit()
        with pytest.raises(InvalidVProgramRuntime):
            await resolve_runtime_bundle_ref(session, storage_service, ipfs_ref)


@pytest.mark.asyncio
async def test_resolve_bundle_ref_rejects_manifest_with_unrecognised_file_hash(
    session_factory: DbSessionFactory, mocker
):
    """A pin whose file hash is neither a 64-char hex hash nor an IPFS CID
    must be rejected via `UnknownHashError`, not silently treated as IPFS."""
    bad_ref = "face" * 16
    bad_file_hash = "not-a-hash"
    storage_service = make_storage_service(mocker, {})
    with session_factory() as session:
        pin_manifest(session, item_hash=bad_ref, file_hash=bad_file_hash)
        session.commit()
        with pytest.raises(InvalidVProgramRuntime):
            await resolve_runtime_bundle_ref(session, storage_service, bad_ref)


def test_runtime_bundle_volume_shape():
    volume = runtime_bundle_volume(BUNDLE_REF)
    assert volume.cost_type == CostType.EXECUTION_VPROGRAM_VOLUME
    assert volume.ref == BUNDLE_REF
    assert volume.use_latest is False
    assert volume.name == "runtime"
