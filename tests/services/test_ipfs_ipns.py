import asyncio
import base64
import datetime as dt
import sys
from unittest.mock import MagicMock

import aioipfs
import pytest

from aleph.services.ipfs.service import (
    InvalidIpnsRecordError,
    IpfsService,
    IpnsRecordInfo,
    IpnsResolutionError,
)

IPNS_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8"
CID = "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB"

INSPECT_OK = {
    "Entry": {
        "Value": f"/ipfs/{CID}",
        "ValidityType": 0,
        "Validity": "2028-06-09T00:00:00Z",
        "Sequence": 2,
    },
    "Validation": {"Valid": True, "Reason": ""},
}


@pytest.fixture
def ipfs_service(mocker):
    return IpfsService(
        ipfs_client=mocker.AsyncMock(), pinning_client=mocker.AsyncMock()
    )


@pytest.mark.asyncio
async def test_verify_ipns_record_ok(ipfs_service, mocker):
    ipfs_service.storage_client.name.inspect = mocker.AsyncMock(return_value=INSPECT_OK)
    info = await ipfs_service.verify_ipns_record(b"\x0a\x01raw", IPNS_NAME)
    assert info == IpnsRecordInfo(
        value_cid=CID,
        sequence=2,
        validity=dt.datetime(2028, 6, 9, tzinfo=dt.timezone.utc),
    )
    _, kwargs = ipfs_service.storage_client.name.inspect.call_args
    assert kwargs["verify"] == IPNS_NAME
    assert kwargs["dump"] is False


@pytest.mark.asyncio
async def test_verify_ipns_record_invalid_signature(ipfs_service, mocker):
    bad = {**INSPECT_OK, "Validation": {"Valid": False, "Reason": "bad signature"}}
    ipfs_service.storage_client.name.inspect = mocker.AsyncMock(return_value=bad)
    with pytest.raises(InvalidIpnsRecordError, match="bad signature"):
        await ipfs_service.verify_ipns_record(b"\x0a\x01raw", IPNS_NAME)


@pytest.mark.asyncio
async def test_verify_ipns_record_rejects_subpath_value(ipfs_service, mocker):
    sub = {
        **INSPECT_OK,
        "Entry": {**INSPECT_OK["Entry"], "Value": f"/ipfs/{CID}/sub/path"},
    }
    ipfs_service.storage_client.name.inspect = mocker.AsyncMock(return_value=sub)
    with pytest.raises(InvalidIpnsRecordError, match="sub-path"):
        await ipfs_service.verify_ipns_record(b"\x0a\x01raw", IPNS_NAME)


@pytest.mark.asyncio
async def test_resolve_ipns_record_ok(ipfs_service, mocker):
    raw = b"\x0a\x01record-bytes"
    ipfs_service.storage_client.routing.get = mocker.AsyncMock(
        return_value={"Extra": base64.b64encode(raw).decode(), "Type": 5}
    )
    record = await ipfs_service.resolve_ipns_record(IPNS_NAME, timeout=5)
    assert record == raw
    ipfs_service.storage_client.routing.get.assert_awaited_once_with(
        f"/ipns/{IPNS_NAME}"
    )


@pytest.mark.asyncio
async def test_resolve_ipns_record_not_found(ipfs_service, mocker):
    ipfs_service.storage_client.routing.get = mocker.AsyncMock(return_value=None)
    with pytest.raises(IpnsResolutionError):
        await ipfs_service.resolve_ipns_record(IPNS_NAME, timeout=5)


@pytest.mark.asyncio
async def test_verify_ipns_record_rejects_non_ipfs_value(ipfs_service, mocker):
    bad = {
        **INSPECT_OK,
        "Entry": {**INSPECT_OK["Entry"], "Value": f"/ipns/{IPNS_NAME}"},
    }
    ipfs_service.storage_client.name.inspect = mocker.AsyncMock(return_value=bad)
    with pytest.raises(InvalidIpnsRecordError, match="unsupported"):
        await ipfs_service.verify_ipns_record(b"\x0a\x01raw", IPNS_NAME)


@pytest.mark.asyncio
async def test_resolve_ipns_record_timeout(ipfs_service, mocker):
    async def hang(*args, **kwargs):
        await asyncio.sleep(10)

    ipfs_service.storage_client.routing.get = hang
    with pytest.raises(IpnsResolutionError):
        await ipfs_service.resolve_ipns_record(IPNS_NAME, timeout=0)


@pytest.mark.asyncio
async def test_resolve_ipns_record_api_error(ipfs_service, mocker):
    ipfs_service.storage_client.routing.get = mocker.AsyncMock(
        side_effect=aioipfs.APIError()
    )
    with pytest.raises(IpnsResolutionError):
        await ipfs_service.resolve_ipns_record(IPNS_NAME, timeout=5)


@pytest.mark.asyncio
async def test_put_ipns_record_posts_multipart(ipfs_service, mocker):
    routing = ipfs_service.storage_client.routing
    routing.url = mocker.Mock(return_value="http://ipfs/api/v0/routing/put")
    routing.post = mocker.AsyncMock(return_value={})

    # Mock tempfile
    mock_file = MagicMock()
    mock_file.name = "/tmp/mock_file"
    mock_tempfile = MagicMock()
    mock_tempfile.__enter__.return_value = mock_file
    mock_tempfile.__exit__.return_value = None
    mocker.patch("tempfile.NamedTemporaryFile", return_value=mock_tempfile)

    # Mock aioipfs.multi.bytes_payload_from_file
    mock_payload = MagicMock()
    mocker.patch("aioipfs.multi.bytes_payload_from_file", return_value=mock_payload)

    # Mock FormDataWriter
    mock_writer = MagicMock()
    mock_writer.__enter__.return_value = mock_writer
    mock_writer.__exit__.return_value = None
    mocker.patch("aioipfs.multi.FormDataWriter", return_value=mock_writer)

    await ipfs_service.put_ipns_record(IPNS_NAME, b"\x0a\x01record-bytes")

    routing.url.assert_called_once_with("routing/put")
    args, kwargs = routing.post.call_args
    assert args[0] == "http://ipfs/api/v0/routing/put"
    assert kwargs["params"] == {"arg": f"/ipns/{IPNS_NAME}"}
    assert kwargs["outformat"] == "json"
