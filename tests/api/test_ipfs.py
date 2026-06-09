import datetime as dt
import hashlib
import json
from decimal import Decimal
from io import BytesIO
from typing import Any
from unittest.mock import AsyncMock as MockAsyncMock

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from aleph_message.models import Chain
from car_test_utils import build_carv1
from in_memory_storage_engine import InMemoryStorageEngine

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.files import get_file
from aleph.db.models import AlephBalanceDb, AlephCreditBalanceDb, GracePeriodFilePinDb
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

# Same message but credit-paid. Used to exercise the credit-tier branch in
# the API-level balance check.
IPFS_MESSAGE_DICT_CREDIT: dict[str, Any] = {
    **IPFS_MESSAGE_DICT,
    "item_content": json.dumps(
        {
            "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
            "time": 1692193373.714271,
            "item_type": "ipfs",
            "item_hash": EXPECTED_FILE_CID,
            "mime_type": "application/octet-stream",
            "payment": {"type": "credit", "chain": "ETH"},
        }
    ),
    # Outer item_hash = sha256(item_content), required by pydantic.
    "item_hash": "f87f7b55a8398d8980ef00267b5df574b22dfeb6e6ecf41ee219bf4da00d8217",
}


@pytest_asyncio.fixture
async def api_client(ccn_test_aiohttp_app, mocker, aiohttp_client):
    ipfs_service = mocker.AsyncMock()
    ipfs_service.add_bytes = mocker.AsyncMock(return_value=EXPECTED_FILE_CID)
    ipfs_service.storage_client.files.stat = mocker.AsyncMock(
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
    assert response.headers.get("Deprecation") == "true"
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
    assert "Deprecation" not in response.headers
    payload = await response.json()
    assert payload["hash"] == EXPECTED_FILE_CID

    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        # Authenticated uploads must get a grace pin to bridge the gap until
        # the STORE message is processed and creates the permanent pin.
        assert _has_grace_period(session, EXPECTED_FILE_CID)


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
    body = await response.text()
    assert response.status == 403, body
    # 403 is also produced by HTTPForbidden from other middleware. Spot-check
    # that the body contains the verifier's reason so the test can't pass on
    # an unrelated 403.
    assert "Forbidden" in body
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
    body = await response.text()
    assert response.status == 422, body
    assert "item_type=ipfs" in body
    ipfs_service.add_bytes.assert_not_called()


@pytest.mark.asyncio
async def test_auth_upload_exceeding_authenticated_cap(
    api_client, session_factory: DbSessionFactory, mocker, mock_config
):
    """Authenticated upload above ipfs.max_upload_file_size returns 413."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )
    ipfs_service = _get_ipfs_service_mock(api_client)

    # Pin the cap to a small value so the test stays fast and stays independent
    # of the production default, then send a payload one byte over it. The test
    # fixture doesn't care about content since ipfs_service.add_bytes is mocked.
    cap = 1 * 1024 * 1024
    mock_config.ipfs.max_upload_file_size.value = cap
    oversized = b"x" * (cap + 1)
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(oversized))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 413, await response.text()
    ipfs_service.add_bytes.assert_not_called()


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
        # Outer item_hash = sha256(item_content), required by pydantic.
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
    body = await response.text()
    assert response.status == 422, body
    # Confirm the 422 came from the controller's CID match check, not from
    # pydantic rejecting the message earlier on a different invariant.
    assert "File hash does not match" in body

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
    Authenticated upload with insufficient balance returns 402 BEFORE the
    file is pinned, regardless of size. No IPFS write, no file row, no
    grace-period side effect.
    """
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )
    ipfs_service = _get_ipfs_service_mock(api_client)

    # No balance row inserted; balance is zero. File is tiny, well under
    # the legacy 25 MiB unauth threshold, to prove size doesn't matter.
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 402, await response.text()

    ipfs_service.add_bytes.assert_not_called()
    with session_factory() as session:
        assert get_file(session=session, file_hash=EXPECTED_FILE_CID) is None
        assert not _has_grace_period(session, EXPECTED_FILE_CID)


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


@pytest.mark.asyncio
async def test_auth_upload_stat_timeout_applies_grace(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Stat timeout after pin returns 504 and applies grace period."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )
    ipfs_service = _get_ipfs_service_mock(api_client)
    # Make stat raise TimeoutError as if asyncio.wait_for timed out.
    ipfs_service.storage_client.files.stat = mocker.AsyncMock(
        side_effect=TimeoutError()
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
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 504, await response.text()

    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        assert _has_grace_period(session, EXPECTED_FILE_CID)


@pytest.mark.asyncio
async def test_auth_credit_upload_insufficient_credit_balance(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    Credit-paid STORE with zero credit balance is rejected pre-pin with 402.
    An ALEPH token balance is irrelevant: the credit branch reads credits
    only. The address gets a large ALEPH balance to make this explicit.
    """
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )
    ipfs_service = _get_ipfs_service_mock(api_client)

    # ALEPH balance must not satisfy a credit-paid request.
    with session_factory() as session:
        session.add(
            AlephBalanceDb(
                address="0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                chain=Chain.ETH,
                balance=Decimal(1_000_000),
                eth_height=0,
            )
        )
        session.commit()

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT_CREDIT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 402, await response.text()

    ipfs_service.add_bytes.assert_not_called()
    with session_factory() as session:
        assert get_file(session=session, file_hash=EXPECTED_FILE_CID) is None
        assert not _has_grace_period(session, EXPECTED_FILE_CID)


@pytest.mark.asyncio
async def test_auth_credit_upload_sufficient_credit_balance(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    Credit-paid STORE with enough credits to cover the 1-day minimum
    runtime passes the API check and the file is pinned.
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

    # Seed the credit cache directly. The amount is well above one day of
    # storage cost for a 34-byte file, so the 1-day minimum runtime check
    # in validate_balance_for_payment is satisfied.
    with session_factory() as session:
        session.add(
            AlephCreditBalanceDb(
                address="0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                credit_ref="test-credit-ref",
                credit_index=0,
                amount_remaining=1_000_000_000,
                expiration_date=None,
                message_timestamp=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.commit()

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT_CREDIT, "sync": True}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 200, await response.text()
    payload = await response.json()
    assert payload["hash"] == EXPECTED_FILE_CID


# ---------------------------------------------------------------------------
# CAR upload tests (POST /api/v0/ipfs/add_car)
# ---------------------------------------------------------------------------

IPFS_ADD_CAR_URI = "/api/v0/ipfs/add_car"

# Stable test root CID: CIDv1 dag-pb with sha2-256 multihash of a known
# byte string. We do NOT need this CID's blocks to actually exist on the
# test kubo: the IpfsService is mocked, so dag_import returns whatever we
# instruct it to return.
DIR_ROOT_CID = "bafybeibwzifw72ttrkqglhi64gn3stoyjs6t2vcyfzr67gqkogfgcyo3uy"
MOCK_DIR_SIZE = 4096  # CumulativeSize returned by the mocked stat


def _build_dir_store_message(item_hash: str) -> dict:
    """Build an IPFS STORE message dict referencing item_hash. Signature is
    mocked away so the placeholder signature is fine."""
    item_content = json.dumps(
        {
            "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
            "time": 1692193373.714271,
            "item_type": "ipfs",
            "item_hash": item_hash,
            "mime_type": "application/octet-stream",
        }
    )
    # Outer item_hash must equal sha256(item_content) or pydantic rejects
    # the message before our handler runs. Compute it here so the test
    # message validates.
    outer_hash = hashlib.sha256(item_content.encode("utf-8")).hexdigest()
    return {
        "chain": "ETH",
        "sender": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
        "type": "STORE",
        "channel": "null",
        "signature": "0x" + "00" * 65,
        "time": 1692193373.7144432,
        "item_type": "inline",
        "item_content": item_content,
        "item_hash": outer_hash,
    }


@pytest_asyncio.fixture
async def api_client_with_dag_import(api_client):
    """Extends `api_client`: also mock dag_import to return DIR_ROOT_CID and
    update files.stat to return directory-shaped stats."""
    ipfs_service = _get_ipfs_service_mock(api_client)
    ipfs_service.dag_import = MockAsyncMock(return_value=[DIR_ROOT_CID])
    ipfs_service.storage_client.files.stat = MockAsyncMock(
        return_value={
            "Hash": DIR_ROOT_CID,
            "Size": 0,
            "CumulativeSize": MOCK_DIR_SIZE,
            "Blocks": 4,
            "Type": "directory",
        }
    )
    return api_client


@pytest.mark.asyncio
async def test_add_car_success(
    api_client_with_dag_import,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
) -> None:
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

    car_bytes = build_carv1(DIR_ROOT_CID)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="dir.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": True}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 200, body
    payload = await response.json()
    assert payload == {
        "status": "success",
        "hash": DIR_ROOT_CID,
        "size": MOCK_DIR_SIZE,
    }

    with session_factory() as session:
        file = get_file(session=session, file_hash=DIR_ROOT_CID)
        assert file is not None
        assert file.type == FileType.DIRECTORY
        assert file.size == MOCK_DIR_SIZE
        # Authenticated success: no grace period.
        assert not _has_grace_period(session, DIR_ROOT_CID)

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_called_once()


@pytest.mark.asyncio
async def test_add_car_missing_metadata(api_client_with_dag_import) -> None:
    car_bytes = build_carv1(DIR_ROOT_CID)
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="dir.car")

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 422, body
    assert "metadata is required" in body

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_not_called()


@pytest.mark.asyncio
async def test_add_car_missing_file(api_client_with_dag_import) -> None:
    message = _build_dir_store_message(DIR_ROOT_CID)
    form_data = aiohttp.FormData()
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 422, body
    assert "Missing 'file'" in body

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_not_called()


@pytest.mark.asyncio
async def test_add_car_invalid_signature(api_client_with_dag_import, mocker) -> None:
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
        side_effect=web.HTTPForbidden(),
    )

    car_bytes = build_carv1(DIR_ROOT_CID)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="dir.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    assert response.status == 403, await response.text()

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_not_called()


@pytest.mark.asyncio
async def test_add_car_wrong_item_type(api_client_with_dag_import, mocker) -> None:
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # Build a message with item_type=storage instead of ipfs.
    item_content = json.dumps(
        {
            "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
            "time": 1692193373.714271,
            "item_type": "storage",
            "item_hash": "0214e5578f5acb5d36ea62255cbf1157a4bdde7b9612b5db4899b2175e310b6f",
            "mime_type": "application/octet-stream",
        }
    )
    bad_message = {
        **IPFS_MESSAGE_DICT,
        "item_content": item_content,
        "item_hash": hashlib.sha256(item_content.encode("utf-8")).hexdigest(),
    }

    car_bytes = build_carv1(DIR_ROOT_CID)
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="dir.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": bad_message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 422, body
    assert "item_type=ipfs" in body

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_not_called()


@pytest.mark.asyncio
async def test_add_car_v2_rejected(api_client_with_dag_import, mocker) -> None:
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    car_bytes = build_carv1(DIR_ROOT_CID, version=2)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="v2.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 422, body
    assert "unsupported CAR version" in body

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_not_called()


@pytest.mark.asyncio
async def test_add_car_multi_root_rejected(api_client_with_dag_import, mocker) -> None:
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    car_bytes = build_carv1(DIR_ROOT_CID, n_roots=2)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="multi.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 422, body
    assert "expected exactly 1 root" in body

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_not_called()


@pytest.mark.asyncio
async def test_add_car_header_root_mismatch(api_client_with_dag_import, mocker) -> None:
    """CAR header declares a root that does not match the metadata.
    Expected: 422 'Root CID does not match', no kubo contact."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    other_root = "bafybeicndzezzx7zyvuoukheebsegjnokf3vlwm4nlz77pnxllgr2jjelu"
    car_bytes = build_carv1(other_root)
    # Metadata still claims DIR_ROOT_CID.
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="mismatch.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 422, body
    assert "Root CID does not match" in body

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_not_called()


@pytest.mark.asyncio
async def test_add_car_imported_root_mismatch(
    api_client_with_dag_import,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
) -> None:
    """CAR header claims DIR_ROOT_CID and metadata agrees, but mocked
    dag_import returns a different root (simulating a lying header).
    Expected: 422 'Imported root does not match expected'. Grace period not
    written because we never recorded `cid` in this code path."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # Force dag_import to return a different CID than expected.
    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import = MockAsyncMock(
        return_value=["bafybeicndzezzx7zyvuoukheebsegjnokf3vlwm4nlz77pnxllgr2jjelu"]
    )

    # Seed enough balance to clear the pre-import gate so the test can
    # exercise the post-import mismatch path.
    with session_factory() as session:
        session.add(
            AlephBalanceDb(
                address="0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                chain=Chain.ETH,
                balance=Decimal(1_000_000),
                eth_height=0,
            )
        )
        session.commit()

    car_bytes = build_carv1(DIR_ROOT_CID)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="lying.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 422, body
    assert "Imported root does not match expected" in body


@pytest.mark.asyncio
async def test_add_car_too_large(
    api_client_with_dag_import, mocker, mock_config
) -> None:
    """CAR larger than max_upload_car_size returns 413, no kubo contact."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )
    # Shrink the cap to make the test fast: 1 KiB.
    mock_config.ipfs.max_upload_car_size.value = 1024

    # Payload larger than 1 KiB but with a valid header. The size check
    # raises mid-stream.
    car_header = build_carv1(DIR_ROOT_CID)
    car_bytes = car_header + (b"\x00" * 4096)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="huge.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    assert response.status == 413, await response.text()

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import.assert_not_called()


@pytest.mark.asyncio
async def test_add_car_ipfs_disabled(
    ccn_test_aiohttp_app, mocker, aiohttp_client
) -> None:
    """If ipfs_service is None in app state, return 403."""
    ccn_test_aiohttp_app[APP_STATE_STORAGE_SERVICE] = StorageService(
        storage_engine=InMemoryStorageEngine(files={}),
        ipfs_service=None,  # type: ignore[arg-type]
        node_cache=mocker.AsyncMock(),
    )
    ccn_test_aiohttp_app[APP_STATE_SIGNATURE_VERIFIER] = SignatureVerifier()

    client = await aiohttp_client(ccn_test_aiohttp_app)
    car_bytes = build_carv1(DIR_ROOT_CID)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="dir.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await client.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 403, body
    assert "IPFS is disabled" in body


@pytest.mark.asyncio
async def test_add_car_insufficient_balance(
    api_client_with_dag_import,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
) -> None:
    """Authenticated CAR upload with zero balance is rejected pre-import:
    402, dag_import not called, no file row, no grace period."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # No AlephBalanceDb row → balance is zero.
    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)

    car_bytes = build_carv1(DIR_ROOT_CID)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="dir.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    assert response.status == 402, await response.text()

    ipfs_service.dag_import.assert_not_called()
    with session_factory() as session:
        assert get_file(session=session, file_hash=DIR_ROOT_CID) is None
        assert not _has_grace_period(session, DIR_ROOT_CID)


@pytest.mark.asyncio
async def test_add_car_stat_timeout(
    api_client_with_dag_import,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
) -> None:
    """If files.stat times out post-import, return 504 and apply grace
    period with fallback size = CAR file size on disk."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    with session_factory() as session:
        session.add(
            AlephBalanceDb(
                address="0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                chain=Chain.ETH,
                balance=Decimal(1_000_000),
                eth_height=0,
            )
        )
        session.commit()

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.storage_client.files.stat = MockAsyncMock(
        side_effect=TimeoutError(),
    )

    car_bytes = build_carv1(DIR_ROOT_CID)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="dir.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 504, body
    assert "Timed out waiting for IPFS stat" in body

    with session_factory() as session:
        file = get_file(session=session, file_hash=DIR_ROOT_CID)
        assert file is not None
        assert file.type == FileType.DIRECTORY
        # Fallback size = CAR bytes on disk (just the header here).
        assert file.size == len(car_bytes)
        assert _has_grace_period(session, DIR_ROOT_CID)


@pytest.mark.asyncio
async def test_add_car_dag_import_failure(
    api_client_with_dag_import,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
) -> None:
    """kubo failure during dag_import returns 502, no file row written."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    with session_factory() as session:
        session.add(
            AlephBalanceDb(
                address="0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                chain=Chain.ETH,
                balance=Decimal(1_000_000),
                eth_height=0,
            )
        )
        session.commit()

    ipfs_service = _get_ipfs_service_mock(api_client_with_dag_import)
    ipfs_service.dag_import = MockAsyncMock(
        side_effect=RuntimeError("kubo went away"),
    )

    car_bytes = build_carv1(DIR_ROOT_CID)
    message = _build_dir_store_message(DIR_ROOT_CID)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(car_bytes), filename="dir.car")
    form_data.add_field(
        "metadata",
        json.dumps({"message": message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client_with_dag_import.post(IPFS_ADD_CAR_URI, data=form_data)
    body = await response.text()
    assert response.status == 502, body
    assert "Failed to import CAR into IPFS" in body

    with session_factory() as session:
        assert get_file(session=session, file_hash=DIR_ROOT_CID) is None
