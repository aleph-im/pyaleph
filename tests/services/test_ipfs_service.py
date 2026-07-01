import asyncio
import json
from unittest.mock import AsyncMock, patch

import aioipfs
import pytest

from aleph.services.ipfs.service import IpfsService
from aleph.types.message_status import FileUnavailable


@pytest.mark.asyncio
async def test_get_ipfs_size_data_filesize():
    """Test when dag.get returns a dict with Data.filesize"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(return_value={"Data": {"filesize": 1234}})
    service = IpfsService(ipfs_client=ipfs_client)

    # Execute
    result = await service.get_ipfs_size("test_hash")

    # Assert
    assert result == 1234
    ipfs_client.dag.get.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_data_tsize():
    """Test when dag.get returns a dict with Data.Tsize"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(return_value={"Data": {"Tsize": 2345}})
    service = IpfsService(ipfs_client=ipfs_client)

    # Execute
    result = await service.get_ipfs_size("test_hash")

    # Assert
    assert result == 2345
    ipfs_client.dag.get.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_tsize():
    """Test when dag.get returns a dict with top level Tsize"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(return_value={"Data": {}, "Tsize": 3456})
    service = IpfsService(ipfs_client=ipfs_client)

    # Execute
    result = await service.get_ipfs_size("test_hash")

    # Assert
    assert result == 3456
    ipfs_client.dag.get.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_links():
    """Test when dag.get returns a dict with Links array containing Tsize values"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(
        return_value={"Links": [{"Tsize": 100}, {"Tsize": 200}, {"Tsize": 300}]}
    )
    service = IpfsService(ipfs_client=ipfs_client)

    # Execute
    result = await service.get_ipfs_size("test_hash")

    # Assert
    assert result == 600  # Sum of all Tsize values
    ipfs_client.dag.get.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_links_invalid():
    """Test when dag.get returns a dict with Links containing invalid entries"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(
        return_value={
            "Links": [
                {"Tsize": 100},
                {"NoTsize": "invalid"},  # Missing Tsize
                {"Tsize": 300},
            ]
        }
    )
    service = IpfsService(ipfs_client=ipfs_client)

    # Execute
    result = await service.get_ipfs_size("test_hash")

    # Assert
    assert result == 400  # Sum of valid Tsize values
    ipfs_client.dag.get.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_size():
    """Test when dag.get returns a dict with Size field"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(return_value={"Size": 4567})
    service = IpfsService(ipfs_client=ipfs_client)

    # Execute
    result = await service.get_ipfs_size("test_hash")

    # Assert
    assert result == 4567
    ipfs_client.dag.get.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_non_dict():
    """Test when dag.get returns a non-dictionary (bytes)"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(return_value=b"raw data")
    ipfs_client.block.stat = AsyncMock(return_value={"Size": 8765})

    service = IpfsService(ipfs_client=ipfs_client)

    # Execute
    result = await service.get_ipfs_size("test_hash")

    # Assert
    assert result == 8765
    ipfs_client.dag.get.assert_called_once_with("test_hash")
    ipfs_client.block.stat.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_json():
    """Test when dag.get returns a non-dictionary (bytes)"""
    # Setup
    mock_dag_get_data = {
        "Data": {"/": {"bytes": "CAE"}},
        "Links": [
            {"Hash": {"/": "hash1"}, "Name": "test1", "Tsize": 12596071},
            {"Hash": {"/": "hash2"}, "Name": "test2", "Tsize": 20168090},
        ],
    }
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(return_value=json.dumps(mock_dag_get_data))

    service = IpfsService(ipfs_client=ipfs_client)

    # Execute
    result = await service.get_ipfs_size("test_hash")

    # Assert
    assert result == 32764161
    ipfs_client.dag.get.assert_called_once_with("test_hash")
    ipfs_client.block.stat.assert_not_called()


@pytest.mark.asyncio
async def test_get_ipfs_size_api_error():
    """Test handling of APIError"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(side_effect=aioipfs.APIError("API Error"))

    service = IpfsService(ipfs_client=ipfs_client)

    # Execute with single try
    result = await service.get_ipfs_size("test_hash", tries=1)

    # Assert
    assert result is None
    ipfs_client.dag.get.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_api_error_multiple_tries():
    """Test handling of APIError with multiple tries"""
    # Setup
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(side_effect=aioipfs.APIError("API Error"))

    service = IpfsService(ipfs_client=ipfs_client)

    # Mock asyncio.sleep to not actually sleep during test
    with patch("asyncio.sleep", new_callable=AsyncMock):
        # Execute with multiple tries
        result = await service.get_ipfs_size("test_hash", tries=3)

    # Assert
    assert result is None
    assert ipfs_client.dag.get.call_count == 3


@pytest.mark.asyncio
async def test_get_ipfs_size_timeout_error():
    """Test handling of TimeoutError"""
    # Setup
    ipfs_client = AsyncMock()
    # Using side_effect to simulate timeout
    ipfs_client.dag.get = AsyncMock(side_effect=asyncio.TimeoutError())

    service = IpfsService(ipfs_client=ipfs_client)

    # A single try should raise FileUnavailable immediately on timeout.
    with pytest.raises(FileUnavailable):
        await service.get_ipfs_size("test_hash")

    ipfs_client.dag.get.assert_called_once_with("test_hash")


@pytest.mark.asyncio
async def test_get_ipfs_size_timeout_error_multiple_tries():
    """Timeouts should retry up to `tries` times before raising FileUnavailable."""
    ipfs_client = AsyncMock()
    ipfs_client.dag.get = AsyncMock(side_effect=asyncio.TimeoutError())

    service = IpfsService(ipfs_client=ipfs_client)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(FileUnavailable):
            await service.get_ipfs_size("test_hash", tries=3)

    # Should have attempted exactly `tries` times before giving up
    assert ipfs_client.dag.get.call_count == 3


@pytest.mark.asyncio
async def test_get_ipfs_size_cancelled_error():
    """Test handling of CancelledError"""
    # Setup
    ipfs_client = AsyncMock()
    # Using side_effect to simulate cancellation
    ipfs_client.dag.get = AsyncMock(side_effect=asyncio.CancelledError())

    service = IpfsService(ipfs_client=ipfs_client)

    with pytest.raises(asyncio.CancelledError):
        # Mock asyncio.sleep to not actually sleep during test
        with patch("asyncio.sleep", new_callable=AsyncMock):
            # Execute
            result = await service.get_ipfs_size("test_hash", tries=2)

        # Assert
        assert result is None
        # Should be called twice since CancelledError doesn't count as a try
        assert ipfs_client.dag.get.call_count == 2


@pytest.mark.asyncio
async def test_get_ipfs_size_success_after_retry():
    """Test successful retrieval after initial failures"""
    # Setup
    ipfs_client = AsyncMock()
    # First call fails, second call succeeds
    ipfs_client.dag.get = AsyncMock(
        side_effect=[aioipfs.APIError("API Error"), {"Data": {"filesize": 9876}}]
    )

    service = IpfsService(ipfs_client=ipfs_client)

    # Mock asyncio.sleep to not actually sleep during test
    with patch("asyncio.sleep", new_callable=AsyncMock):
        # Execute with multiple tries
        result = await service.get_ipfs_size("test_hash", tries=3)

    # Assert
    assert result == 9876
    assert ipfs_client.dag.get.call_count == 2


@pytest.mark.asyncio
async def test_add_file_streams_from_path_and_returns_cid():
    """add_file streams the file from its path via aioipfs.add (an async
    generator) instead of buffering it in memory, and returns the CID."""
    calls = []

    async def fake_add(*files, **kwargs):
        calls.append((files, kwargs))
        # aioipfs.add yields one entry per file added.
        yield {"Name": files[0], "Hash": "QmStreamedCid", "Size": "34"}

    ipfs_client = AsyncMock()
    ipfs_client.add = fake_add
    service = IpfsService(ipfs_client=ipfs_client)

    cid = await service.add_file("/tmp/upload.bin")

    assert cid == "QmStreamedCid"
    # The path is forwarded as a string; add_bytes must not be used.
    assert calls == [(("/tmp/upload.bin",), {"cid_version": 0})]
    ipfs_client.add_bytes.assert_not_called()


@pytest.mark.asyncio
async def test_add_file_raises_when_no_entry_returned():
    """If aioipfs.add yields nothing (should not happen for a real file), we
    fail loudly rather than returning a bogus CID."""

    async def empty_add(*files, **kwargs):
        return
        yield  # pragma: no cover - marks this as an async generator

    ipfs_client = AsyncMock()
    ipfs_client.add = empty_add
    service = IpfsService(ipfs_client=ipfs_client)

    with pytest.raises(ValueError):
        await service.add_file("/tmp/upload.bin")
