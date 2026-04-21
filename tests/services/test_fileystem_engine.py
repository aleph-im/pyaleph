"""Tests for FileSystemStorageEngine's durable atomic write.

These tests cover the invariants provided by ``write()``:
- Content on disk matches what was written (happy path).
- A temp file is used and atomically renamed.
- ``os.fsync`` is called on the file fd before ``os.replace``.
- Failures during rename leave the target unchanged and clean up the temp file.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from aleph.services.storage.fileystem_engine import FileSystemStorageEngine


@pytest.mark.asyncio
async def test_write_produces_final_file(tmp_path: Path):
    engine = FileSystemStorageEngine(folder=tmp_path)
    await engine.write(filename="abc", content=b"hello")

    final = tmp_path / "abc"
    assert final.is_file()
    assert final.read_bytes() == b"hello"
    assert not (tmp_path / "abc.tmp").exists()


@pytest.mark.asyncio
async def test_write_uses_temp_file_then_rename(tmp_path: Path):
    engine = FileSystemStorageEngine(folder=tmp_path)

    observed = {}
    real_replace = os.replace

    def spy_replace(src, dst):
        observed["src_name"] = os.path.basename(src)
        observed["src_exists_before"] = os.path.exists(src)
        observed["dst_missing_before"] = not os.path.exists(dst)
        return real_replace(src, dst)

    with patch(
        "aleph.services.storage.fileystem_engine.os.replace",
        side_effect=spy_replace,
    ):
        await engine.write(filename="abc", content=b"hello")

    assert observed["src_name"] == "abc.tmp"
    assert observed["src_exists_before"] is True
    assert observed["dst_missing_before"] is True
    assert (tmp_path / "abc").read_bytes() == b"hello"


@pytest.mark.asyncio
async def test_write_fsyncs_before_rename(tmp_path: Path):
    engine = FileSystemStorageEngine(folder=tmp_path)

    call_order = []
    real_fsync = os.fsync
    real_replace = os.replace

    def tracking_fsync(fd):
        call_order.append("fsync")
        return real_fsync(fd)

    def tracking_replace(src, dst):
        call_order.append("replace")
        return real_replace(src, dst)

    with (
        patch(
            "aleph.services.storage.fileystem_engine.os.fsync",
            side_effect=tracking_fsync,
        ),
        patch(
            "aleph.services.storage.fileystem_engine.os.replace",
            side_effect=tracking_replace,
        ),
    ):
        await engine.write(filename="abc", content=b"hello")

    # The file-fd fsync must occur before the atomic rename.
    first_replace = call_order.index("replace")
    assert "fsync" in call_order[:first_replace]


@pytest.mark.asyncio
async def test_write_failure_cleans_up_temp_and_preserves_target(tmp_path: Path):
    engine = FileSystemStorageEngine(folder=tmp_path)

    # Pre-seed the target to confirm it survives a failed write.
    target = tmp_path / "abc"
    target.write_bytes(b"original")

    def boom(src, dst):
        raise OSError("simulated rename failure")

    with patch(
        "aleph.services.storage.fileystem_engine.os.replace",
        side_effect=boom,
    ):
        with pytest.raises(OSError):
            await engine.write(filename="abc", content=b"new-content")

    assert target.read_bytes() == b"original"
    assert not (tmp_path / "abc.tmp").exists()


@pytest.mark.asyncio
async def test_read_missing_returns_none(tmp_path: Path):
    engine = FileSystemStorageEngine(folder=tmp_path)
    assert await engine.read("missing") is None


@pytest.mark.asyncio
async def test_overwrite_replaces_content(tmp_path: Path):
    engine = FileSystemStorageEngine(folder=tmp_path)
    await engine.write(filename="abc", content=b"v1")
    await engine.write(filename="abc", content=b"v2")

    assert (tmp_path / "abc").read_bytes() == b"v2"
    assert not (tmp_path / "abc.tmp").exists()
