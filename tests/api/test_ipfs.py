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
        session.query(GracePeriodFilePinDb).filter_by(file_hash=file_hash).first()
        is not None
    )


@pytest.mark.asyncio
async def test_unauth_upload_happy_path(api_client, session_factory: DbSessionFactory):
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


@pytest.mark.asyncio
async def test_auth_upload_rejects_storage_item_type(
    api_client, session_factory: DbSessionFactory, mocker
):
    """Message with item_type=storage is rejected, no pin happens."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    bad_message = {
        **IPFS_MESSAGE_DICT,
        "item_content": json.dumps(
            {
                "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                "time": 1692193373.714271,
                "item_type": "storage",
                "item_hash": "0214e5578f5acb5d36ea62255cbf1157a4bdde7b9612b5db4899b2175e310b6f",
                "mime_type": "application/octet-stream",
            }
        ),
        # Outer item_hash must match sha256(item_content) or pydantic rejects
        # with 422 before our item_type check runs.
        "item_hash": "ea0ead4b5a3e3d9a7c5e693fd2797ed253a987c4ad80600ee97111a0d8911d87",
    }

    ipfs_service = _get_ipfs_service_mock(api_client)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": bad_message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 422, await response.text()
    ipfs_service.add_bytes.assert_not_called()


@pytest.mark.asyncio
async def test_auth_upload_exceeding_authenticated_cap(
    api_client, session_factory: DbSessionFactory, mocker
):
    """Authenticated upload above max_upload_file_size (100 MiB) returns 413."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # Build a 101 MiB payload. The test fixture doesn't care about content
    # since ipfs_service.add_bytes is mocked.
    oversized = b"x" * (101 * 1024 * 1024)
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(oversized))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 413, await response.text()


@pytest.mark.asyncio
async def test_auth_upload_cid_mismatch(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    If the message.item_hash does not match the CID the daemon produced,
    return 422 and leave the pinned file under a 24 h grace period.
    """
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # Message claims a different CID than the fixture's mocked add_bytes.
    mismatched = {
        **IPFS_MESSAGE_DICT,
        "item_content": json.dumps(
            {
                "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                "time": 1692193373.714271,
                "item_type": "ipfs",
                "item_hash": "QmY8HD3jCfJ5K6t4VYQvkFBni2LpasVusLGkCrtjkfntSA",
                "mime_type": "application/octet-stream",
            }
        ),
        # Outer item_hash = sha256(item_content) — required by pydantic.
        "item_hash": "dcaa4ab4374b11084ca2fea61d9235f06d658f97adb18af8b4bd659adf0377af",
    }

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": mismatched, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 422, await response.text()

    # Pin happened (CID mismatch is a post-pin check), so the file is in DB
    # with a grace period.
    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        assert _has_grace_period(session, EXPECTED_FILE_CID)


@pytest.mark.asyncio
async def test_auth_upload_insufficient_balance(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    File above the unauth threshold with insufficient balance returns 402
    and leaves the pinned file under a 24 h grace period.
    """
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # 26 MiB payload > 25 MiB unauth cap, so balance check fires.
    payload = b"x" * (26 * 1024 * 1024)

    # No balance row inserted → balance is zero.

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(payload))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 402, await response.text()

    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        assert _has_grace_period(session, EXPECTED_FILE_CID)


@pytest.mark.asyncio
async def test_auth_upload_small_file_skips_balance_check(
    api_client, session_factory: DbSessionFactory, mocker
):
    """
    For files below max_unauthenticated_upload_file_size, the balance check
    short-circuits even when balance is zero. Matches /storage/add_file's
    rule: anything you could have uploaded unauth for free doesn't need a
    balance check.

    We deliberately do NOT mock _verify_user_balance: its internal threshold
    short-circuit is exactly what we want to exercise. Zero balance + tiny
    file → request must succeed.
    """
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

    # No balance inserted — balance is zero. File is 34 bytes << 25 MiB.
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": True}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 200, await response.text()


@pytest.mark.asyncio
async def test_auth_upload_malformed_metadata(
    api_client, session_factory: DbSessionFactory
):
    """Garbage in `metadata` returns 422 and does not pin."""
    ipfs_service = _get_ipfs_service_mock(api_client)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field("metadata", "not-json-at-all", content_type="application/json")

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 422, await response.text()
    ipfs_service.add_bytes.assert_not_called()
