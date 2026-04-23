import json
from decimal import Decimal
from io import BytesIO
from typing import Any

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from aleph_message.models import Chain
from in_memory_storage_engine import InMemoryStorageEngine

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.files import get_file
from aleph.db.models import AlephBalanceDb, GracePeriodFilePinDb
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import MessageStatus
from aleph.web.controllers.app_state_getters import (
    APP_STATE_SIGNATURE_VERIFIER,
    APP_STATE_STORAGE_SERVICE,
)
from aleph.web.controllers.utils import BroadcastStatus, PublicationStatus

IPFS_ADD_FILE_URI = "/api/v0/ipfs/add_file"

FILE_CONTENT = b"Hello earthlings, I come in pieces"
EXPECTED_FILE_CID = "QmPoBEaYRf2HDHHFsD7tYkCcSdpLbx5CYDgCgDtW4ywhSK"
MOCK_FILE_SIZE = 34  # length of FILE_CONTENT

# An IPFS STORE message shaped like MESSAGE_DICT in test_storage.py, but with
# item_type=ipfs and item_hash=EXPECTED_FILE_CID. Signature verification is
# mocked away in tests that don't care about it, so this signature is not
# cryptographically valid.
IPFS_MESSAGE_DICT: dict[str, Any] = {
    "chain": "ETH",
    "sender": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
    "type": "STORE",
    "channel": "null",
    "signature": "0x" + "00" * 65,  # placeholder; signature verify is mocked
    "time": 1692193373.7144432,
    "item_type": "inline",
    "item_content": json.dumps(
        {
            "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
            "time": 1692193373.714271,
            "item_type": "ipfs",
            "item_hash": EXPECTED_FILE_CID,
            "mime_type": "application/octet-stream",
        }
    ),
    "item_hash": "6e717bf3296372a4d7b470a1a29ec2694a78a338002f33c94cb2f518e0c1fdb8",
}


@pytest_asyncio.fixture
async def api_client(ccn_test_aiohttp_app, mocker, aiohttp_client):
    ipfs_service = mocker.AsyncMock()
    ipfs_service.add_bytes = mocker.AsyncMock(return_value=EXPECTED_FILE_CID)
    ipfs_service.pinning_client.files.stat = mocker.AsyncMock(
        return_value={
            "Hash": EXPECTED_FILE_CID,
            "Size": MOCK_FILE_SIZE,
            "CumulativeSize": 42,
            "Blocks": 0,
            "Type": "file",
        }
    )

    ccn_test_aiohttp_app[APP_STATE_STORAGE_SERVICE] = StorageService(
        storage_engine=InMemoryStorageEngine(files={}),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    ccn_test_aiohttp_app[APP_STATE_SIGNATURE_VERIFIER] = SignatureVerifier()

    client = await aiohttp_client(ccn_test_aiohttp_app)
    return client


def _get_ipfs_service_mock(api_client):
    """Extract the mocked ipfs_service from the storage service in app state."""
    return api_client.app[APP_STATE_STORAGE_SERVICE].ipfs_service


def _has_grace_period(session, file_hash: str) -> bool:
    """Helper: is there an active grace-period pin for this file?"""
    return (
        session.query(GracePeriodFilePinDb)
        .filter_by(file_hash=file_hash)
        .first()
        is not None
    )


@pytest.mark.asyncio
async def test_unauth_upload_happy_path(
    api_client, session_factory: DbSessionFactory
):
    """Regression: existing unauthenticated upload still works."""
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    body = await response.text()
    assert response.status == 200, body
    payload = await response.json()
    assert payload["status"] == "success"
    assert payload["hash"] == EXPECTED_FILE_CID
    assert payload["size"] == MOCK_FILE_SIZE

    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        assert file.type == FileType.FILE
        assert _has_grace_period(session, EXPECTED_FILE_CID)


@pytest.mark.asyncio
async def test_unauth_upload_exceeding_size_limit(
    api_client, session_factory: DbSessionFactory
):
    """Unauth mode rejects files over max_unauthenticated_upload_file_size."""
    # Default unauth limit is 25 MiB. Send 26 MiB.
    oversized = b"x" * (26 * 1024 * 1024)
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(oversized))

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 413, await response.text()


@pytest.mark.asyncio
async def test_auth_upload_happy_path(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Authenticated upload: small file, valid message, sufficient balance."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "aleph.web.controllers.ipfs.broadcast_and_process_message",
        new_callable=mocker.AsyncMock,
        return_value=BroadcastStatus(
            publication_status=PublicationStatus.from_failures([]),
            message_status=MessageStatus.PROCESSED,
        ),
    )

    with session_factory() as session:
        session.add(
            AlephBalanceDb(
                address="0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                chain=Chain.ETH,
                balance=Decimal(1000),
                eth_height=0,
            )
        )
        session.commit()

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": True}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    body = await response.text()
    assert response.status == 200, body
    payload = await response.json()
    assert payload["hash"] == EXPECTED_FILE_CID

    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        # Authenticated uploads do NOT get a grace period (message anchors).
        assert not _has_grace_period(session, EXPECTED_FILE_CID)


@pytest.mark.asyncio
async def test_auth_upload_bad_signature(
    api_client, session_factory: DbSessionFactory, mocker
):
    """Invalid signature is rejected BEFORE the file is pinned."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
        side_effect=web.HTTPForbidden(),
    )
    # Spy on add_bytes to confirm we never called it.
    ipfs_service = _get_ipfs_service_mock(api_client)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 403, await response.text()
    ipfs_service.add_bytes.assert_not_called()

    with session_factory() as session:
        assert get_file(session=session, file_hash=EXPECTED_FILE_CID) is None
