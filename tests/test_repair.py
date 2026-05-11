import hashlib
from typing import AsyncIterable, Dict, Optional
from unittest.mock import MagicMock

import pytest

from aleph.repair import _fix_corrupt_storage_cache
from aleph.services.storage.engine import StorageEngine
from aleph.storage import StorageService


def _sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class MockStorageEngine(StorageEngine):
    def __init__(self, files: Dict[str, bytes]):
        self.files = files

    async def read(self, filename: str) -> Optional[bytes]:
        return self.files.get(filename)

    async def read_iterator(
        self, filename: str, chunk_size: int = 1024 * 1024
    ) -> Optional[AsyncIterable[bytes]]:
        return None

    async def write(self, filename: str, content: bytes):
        self.files[filename] = content

    async def delete(self, filename: str):
        self.files.pop(filename, None)

    async def exists(self, filename: str) -> bool:
        return filename in self.files


def _mock_session(hashes):
    session = MagicMock()
    file_mocks = [MagicMock(hash=h) for h in hashes]
    session.execute.return_value.scalars.return_value.all.return_value = file_mocks
    return session


@pytest.mark.asyncio
async def test_fix_corrupt_storage_cache_removes_mismatches(mocker):
    """A file whose content does not match its SHA-256 filename is deleted."""
    real_content = b'{"hello": "world"}'
    real_hash = _sha256_hex(real_content)

    storage_engine = MockStorageEngine(files={real_hash: b"not the right content"})
    storage_service = StorageService(
        storage_engine,
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    removed = await _fix_corrupt_storage_cache(_mock_session([real_hash]), storage_service)

    assert removed == 1
    assert real_hash not in storage_engine.files


@pytest.mark.asyncio
async def test_fix_corrupt_storage_cache_keeps_valid_files(mocker):
    """A file whose SHA-256 matches its filename is left untouched."""
    real_content = b'{"status": "ok"}'
    real_hash = _sha256_hex(real_content)

    storage_engine = MockStorageEngine(files={real_hash: real_content})
    storage_service = StorageService(
        storage_engine,
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    removed = await _fix_corrupt_storage_cache(_mock_session([real_hash]), storage_service)

    assert removed == 0
    assert storage_engine.files[real_hash] == real_content


@pytest.mark.asyncio
async def test_fix_corrupt_storage_cache_skips_missing_files(mocker):
    """A hash registered in the DB but absent from local storage is skipped."""
    real_hash = _sha256_hex(b"content not on disk")

    storage_engine = MockStorageEngine(files={})
    storage_service = StorageService(
        storage_engine,
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    removed = await _fix_corrupt_storage_cache(_mock_session([real_hash]), storage_service)

    assert removed == 0


@pytest.mark.asyncio
async def test_fix_corrupt_storage_cache_skips_ipfs_hashes(mocker):
    """IPFS hashes (CIDv0/CIDv1) are not checked — computing their hash requires
    a daemon round-trip."""
    ipfs_hash = "QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ"

    storage_engine = MockStorageEngine(files={ipfs_hash: b"arbitrary bytes"})
    storage_service = StorageService(
        storage_engine,
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    removed = await _fix_corrupt_storage_cache(_mock_session([ipfs_hash]), storage_service)

    assert removed == 0
    assert storage_engine.files[ipfs_hash] == b"arbitrary bytes"
