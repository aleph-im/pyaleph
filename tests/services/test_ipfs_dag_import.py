"""Unit tests for the dag/import NDJSON response parser."""

import pathlib
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import aioipfs
import pytest

from aleph.services.ipfs.service import (
    DagImportError,
    IpfsService,
    parse_dag_import_response,
)


def test_single_root_success() -> None:
    body = b'{"Root":{"Cid":{"/":"bafyabc"},"PinErrorMsg":""}}\n'
    assert parse_dag_import_response(body) == ["bafyabc"]


def test_multiple_roots_preserves_order() -> None:
    body = (
        b'{"Root":{"Cid":{"/":"bafyA"},"PinErrorMsg":""}}\n'
        b'{"Root":{"Cid":{"/":"bafyB"},"PinErrorMsg":""}}\n'
    )
    assert parse_dag_import_response(body) == ["bafyA", "bafyB"]


def test_ignores_non_root_entries() -> None:
    body = (
        b'{"Stats":{"BlockCount":3}}\n'
        b'{"Root":{"Cid":{"/":"bafyXYZ"},"PinErrorMsg":""}}\n'
    )
    assert parse_dag_import_response(body) == ["bafyXYZ"]


def test_skips_blank_lines() -> None:
    body = b"\n" b'{"Root":{"Cid":{"/":"bafyok"},"PinErrorMsg":""}}\n' b"\n"
    assert parse_dag_import_response(body) == ["bafyok"]


def test_pin_error_raises() -> None:
    body = (
        b'{"Root":{"Cid":{"/":"bafybad"},"PinErrorMsg":"context deadline exceeded"}}\n'
    )
    with pytest.raises(DagImportError, match="context deadline exceeded"):
        parse_dag_import_response(body)


def test_no_roots_returns_empty_list() -> None:
    body = b'{"Stats":{"BlockCount":3}}\n'
    assert parse_dag_import_response(body) == []


def test_malformed_json_raises() -> None:
    body = b'{"Root":not-json}\n'
    with pytest.raises(DagImportError, match="malformed NDJSON"):
        parse_dag_import_response(body)


@pytest.mark.asyncio
async def test_dag_import_streams_car_and_parses_response(
    tmp_path: pathlib.Path,
) -> None:
    car_path = tmp_path / "test.car"
    car_path.write_bytes(b"\x00" * 64)  # parser doesn't read this; only kubo does

    # Build a fake aiohttp response.
    response_body = b'{"Root":{"Cid":{"/":"bafyhello"},"PinErrorMsg":""}}\n'
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=response_body)
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_response)

    # MagicMock(spec=aioipfs.AsyncIPFS) won't allow .core because `core` is
    # set in __init__ and is not visible on the class object. Use an
    # unrestricted MagicMock so we can compose the dotted attribute chain
    # the production method walks.
    pinning_client = MagicMock()
    pinning_client.core.driver.session = mock_session
    pinning_client.core.driver.auth = None
    pinning_client.core.url = MagicMock(return_value="http://kubo/api/v0/dag/import")

    service = IpfsService(
        ipfs_client=MagicMock(spec=aioipfs.AsyncIPFS),
        pinning_client=pinning_client,
    )

    result = await service.dag_import(car_path)

    assert result == ["bafyhello"]
    # Verify the request shape.
    mock_session.post.assert_called_once()
    call_kwargs = mock_session.post.call_args.kwargs
    assert call_kwargs["params"]["pin-roots"] == "true"
    assert call_kwargs["params"]["silent"] == "false"

    # Verify the multipart body wiring: aiohttp.FormData with a single
    # "file" field carrying the CAR MIME type.
    form = call_kwargs["data"]
    assert isinstance(form, aiohttp.FormData)
    # FormData._fields is a list of (type_options: MultiDict, headers: dict, value)
    # tuples. type_options carries the "name" and "filename"; headers carries
    # Content-Type. We assert exactly one field named "file" with the CAR
    # content type.
    assert len(form._fields) == 1
    type_options, headers, _value = form._fields[0]
    assert type_options["name"] == "file"
    assert type_options["filename"] == "test.car"
    assert headers["Content-Type"] == "application/vnd.ipld.car"
