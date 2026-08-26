import datetime as dt
import hashlib
import json

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from in_memory_storage_engine import InMemoryStorageEngine
from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH

from aleph.db.accessors.files import insert_message_file_pin
from aleph.db.models import (
    AlephCreditBalanceDb,
    MessageStatusDb,
    PendingMessageDb,
    StoredFileDb,
)
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.schemas.message_content import ContentSource, MessageContent
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import ErrorCode, MessageStatus
from aleph.web.controllers.app_state_getters import APP_STATE_STORAGE_SERVICE

# Note: fixture_vprogram_message and user_credit_balance are redefined here
# (rather than imported from tests/message_processing/test_process_vprograms.py)
# because pytest fixture functions imported across test modules are flagged as
# unused imports by ruff and get stripped by `hatch run linting:fmt`. Keep the
# canonical values identical to the originals.

SENDER = VPROGRAM_CONTENT["address"]

PRICE_URI = f"/api/v0/price/{VPROGRAM_ITEM_HASH}"
PRICE_ESTIMATE_URI = "/api/v0/price/estimate"
MESSAGES_URI = "/api/v0/messages.json"
MESSAGE_URI = f"/api/v0/messages/{VPROGRAM_ITEM_HASH}"


@pytest.fixture
def fixture_vprogram_message(session_factory: DbSessionFactory) -> PendingMessageDb:
    pending_message = PendingMessageDb(
        item_hash=VPROGRAM_ITEM_HASH,
        type=MessageType.v_program,
        chain=Chain.ETH,
        sender=SENDER,
        signature=None,
        item_type=ItemType.inline,
        item_content=json.dumps(VPROGRAM_CONTENT),
        time=timestamp_to_datetime(1719502000.0),
        channel=None,
        reception_time=timestamp_to_datetime(1719502001),
        fetched=True,
        check_message=False,
        retries=0,
        next_attempt=dt.datetime(2026, 1, 1),
    )
    with session_factory() as session:
        session.add(pending_message)
        session.add(
            MessageStatusDb(
                item_hash=pending_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=pending_message.reception_time,
            )
        )
        session.commit()
    return pending_message


def insert_vprogram_refs(session: DbSession) -> None:
    """
    Insert the file pins referenced by the V-Program message so that the
    dependency check passes at processing time.
    """

    refs = {
        VPROGRAM_CONTENT["runtime"]["ref"],
        VPROGRAM_CONTENT["workload"]["ref"],
        VPROGRAM_CONTENT["workload"]["hash_tree"],
    }
    for volume in VPROGRAM_CONTENT["volumes"]:
        refs.add(volume["ref"])
        refs.add(volume["hash_tree"])

    created = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)

    for ref in refs:
        # The file hash just has to be a valid hash: use the reversed ref.
        file_hash = ref[::-1]
        session.add(StoredFileDb(hash=file_hash, size=1024 * 1024, type=FileType.FILE))
        session.flush()
        insert_message_file_pin(
            session=session,
            file_hash=file_hash,
            owner=SENDER,
            item_hash=ref,
            ref=None,
            created=created,
        )


BUNDLE_REF = "ba" * 32
MANIFEST_FILE_HASH = VPROGRAM_CONTENT["runtime"]["ref"][::-1]
MANIFEST = {
    "format": "aleph-vprogram-runtime",
    "format_version": 1,
    "platform": "sev_snp",
    "bundle": {"ref": BUNDLE_REF, "size": 1, "sha256": "00" * 32},
}


def insert_bundle_pin(
    session: DbSession, size_bytes: int = 3 * 1024 * 1024 * 1024
) -> None:
    session.add(
        StoredFileDb(hash=BUNDLE_REF[::-1], size=size_bytes, type=FileType.FILE)
    )
    session.flush()
    insert_message_file_pin(
        session=session,
        file_hash=BUNDLE_REF[::-1],
        owner=SENDER,
        item_hash=BUNDLE_REF,
        ref=None,
        created=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
    )


def store_manifest(message_processor: PendingMessageProcessor, raw: bytes) -> None:
    """Put the manifest bytes where the handler's StorageService reads them."""
    storage_engine = message_processor.message_handler.storage_service.storage_engine
    assert isinstance(storage_engine, InMemoryStorageEngine)
    storage_engine.files[MANIFEST_FILE_HASH] = raw


@pytest.fixture
def user_credit_balance(session_factory: DbSessionFactory) -> None:
    with session_factory() as session:
        session.add(
            AlephCreditBalanceDb(
                address=SENDER,
                credit_ref="test-credit-ref",
                credit_index=0,
                amount_remaining=1_000_000_000,
                expiration_date=None,
                message_timestamp=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.commit()


@pytest.mark.asyncio
async def test_vprogram_price_estimate(
    ccn_api_client,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # The shared ccn_api_client fixture wires up a fully mocked storage
    # service (see tests/conftest.py's ccn_test_aiohttp_app), so inline
    # content resolution needs to be stubbed here to return the actual
    # V-PROGRAM content instead of an unconfigured AsyncMock.
    raw_content = json.dumps(VPROGRAM_CONTENT, separators=(",", ":"))
    storage_service = ccn_api_client.app[APP_STATE_STORAGE_SERVICE]
    storage_service.get_message_content.return_value = MessageContent(
        hash=VPROGRAM_ITEM_HASH,
        source=ContentSource.INLINE,
        value=VPROGRAM_CONTENT,
        raw_value=raw_content,
    )

    message = {
        "chain": "ETH",
        "sender": VPROGRAM_CONTENT["address"],
        "type": "V-PROGRAM",
        "channel": "TEST",
        "time": 1719502000.0,
        "item_type": "inline",
        "item_hash": VPROGRAM_ITEM_HASH,
        "item_content": raw_content,
    }
    response = await ccn_api_client.post(PRICE_ESTIMATE_URI, json={"message": message})
    assert response.status == 200, await response.text()
    result = await response.json()
    assert float(result["cost"]) > 0
    assert result["payment_type"] == "credit"


@pytest.mark.asyncio
async def test_vprogram_price_estimate_with_sizes_includes_artifacts(
    ccn_api_client,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    content = {
        **VPROGRAM_CONTENT,
        "workload": {**VPROGRAM_CONTENT["workload"], "estimated_size_mib": 50 * 1024},
        "runtime_estimated_size_mib": 3 * 1024,
    }
    raw_content = json.dumps(content, separators=(",", ":"))
    # The content differs from VPROGRAM_CONTENT (extra estimated_* fields),
    # so its item_hash must be recomputed: the message-level validator checks
    # item_hash == sha256(item_content).
    item_hash = hashlib.sha256(raw_content.encode()).hexdigest()
    storage_service = ccn_api_client.app[APP_STATE_STORAGE_SERVICE]
    storage_service.get_message_content.return_value = MessageContent(
        hash=item_hash,
        source=ContentSource.INLINE,
        value=content,
        raw_value=raw_content,
    )

    message = {
        "chain": "ETH",
        "sender": VPROGRAM_CONTENT["address"],
        "type": "V-PROGRAM",
        "channel": "TEST",
        "time": 1719502000.0,
        "item_type": "inline",
        "item_hash": item_hash,
        "item_content": raw_content,
    }
    response = await ccn_api_client.post(PRICE_ESTIMATE_URI, json={"message": message})
    assert response.status == 200, await response.text()
    result = await response.json()
    names = {d["name"] for d in result["detail"]}
    assert {"workload", "runtime"} <= names
    workload = next(d for d in result["detail"] if d["name"] == "workload")
    assert workload["type"] == "EXECUTION_VPROGRAM_VOLUME"


@pytest.mark.asyncio
async def test_vprogram_price_estimate_with_runtime_size_skips_resolver(
    ccn_api_client,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # When runtime_estimated_size_mib is provided, the estimate must win: the
    # resolver is never called, and exactly one "runtime" row is billed, sized
    # from the estimate (not doubled by also appending a resolved bundle).
    content = {
        **VPROGRAM_CONTENT,
        "runtime_estimated_size_mib": 3 * 1024,
    }
    raw_content = json.dumps(content, separators=(",", ":"))
    item_hash = hashlib.sha256(raw_content.encode()).hexdigest()
    storage_service = ccn_api_client.app[APP_STATE_STORAGE_SERVICE]
    storage_service.get_message_content.return_value = MessageContent(
        hash=item_hash,
        source=ContentSource.INLINE,
        value=content,
        raw_value=raw_content,
    )

    resolver = mocker.patch(
        "aleph.web.controllers.prices.resolve_runtime_bundle_ref",
        new=mocker.AsyncMock(return_value=BUNDLE_REF),
    )

    message = {
        "chain": "ETH",
        "sender": VPROGRAM_CONTENT["address"],
        "type": "V-PROGRAM",
        "channel": "TEST",
        "time": 1719502000.0,
        "item_type": "inline",
        "item_hash": item_hash,
        "item_content": raw_content,
    }
    response = await ccn_api_client.post(PRICE_ESTIMATE_URI, json={"message": message})
    assert response.status == 200, await response.text()
    result = await response.json()

    resolver.assert_not_called()
    runtime_rows = [d for d in result["detail"] if d["name"] == "runtime"]
    assert len(runtime_rows) == 1
    assert runtime_rows[0]["size_mib"] == 3 * 1024


@pytest.mark.asyncio
async def test_vprogram_price_estimate_without_runtime_size_resolves_bundle(
    ccn_api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # Without an estimate, the resolver must run exactly once and the
    # resolved bundle is billed as a single "runtime" row, sized from its
    # pinned file.
    bundle_size_mib = 2 * 1024
    with session_factory() as session:
        insert_bundle_pin(session, size_bytes=bundle_size_mib * 1024 * 1024)
        session.commit()

    resolver = mocker.patch(
        "aleph.web.controllers.prices.resolve_runtime_bundle_ref",
        new=mocker.AsyncMock(return_value=BUNDLE_REF),
    )

    raw_content = json.dumps(VPROGRAM_CONTENT, separators=(",", ":"))
    storage_service = ccn_api_client.app[APP_STATE_STORAGE_SERVICE]
    storage_service.get_message_content.return_value = MessageContent(
        hash=VPROGRAM_ITEM_HASH,
        source=ContentSource.INLINE,
        value=VPROGRAM_CONTENT,
        raw_value=raw_content,
    )

    message = {
        "chain": "ETH",
        "sender": VPROGRAM_CONTENT["address"],
        "type": "V-PROGRAM",
        "channel": "TEST",
        "time": 1719502000.0,
        "item_type": "inline",
        "item_hash": VPROGRAM_ITEM_HASH,
        "item_content": raw_content,
    }
    response = await ccn_api_client.post(PRICE_ESTIMATE_URI, json={"message": message})
    assert response.status == 200, await response.text()
    result = await response.json()

    resolver.assert_awaited_once()
    runtime_rows = [d for d in result["detail"] if d["name"] == "runtime"]
    assert len(runtime_rows) == 1
    assert runtime_rows[0]["size_mib"] == pytest.approx(bundle_size_mib)


@pytest.mark.asyncio
async def test_vprogram_message_price(
    ccn_api_client,
    session_factory,
    message_processor,
    fixture_vprogram_message,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    with session_factory() as session:
        insert_vprogram_refs(session)
        insert_bundle_pin(session)
        session.commit()
    store_manifest(message_processor, json.dumps(MANIFEST).encode())

    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(PRICE_URI)
    assert response.status == 200, await response.text()
    result = await response.json()
    assert float(result["cost"]) > 0
    assert result["payment_type"] == "credit"


@pytest.mark.asyncio
async def test_vprogram_in_messages_list(
    ccn_api_client,
    session_factory,
    message_processor,
    fixture_vprogram_message,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    with session_factory() as session:
        insert_vprogram_refs(session)
        insert_bundle_pin(session)
        session.commit()
    store_manifest(message_processor, json.dumps(MANIFEST).encode())

    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    # Filter by msgType (singular, deprecated but still supported).
    response = await ccn_api_client.get(MESSAGES_URI, params={"msgType": "V-PROGRAM"})
    assert response.status == 200, await response.text()
    messages = (await response.json())["messages"]
    assert len(messages) == 1
    assert messages[0]["item_hash"] == VPROGRAM_ITEM_HASH
    assert messages[0]["content"]["verification"]["backend"] == "sev_snp"

    # Filter by msgTypes (plural).
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgTypes": "V-PROGRAM,INSTANCE"}
    )
    assert response.status == 200, await response.text()
    assert any(
        m["item_hash"] == VPROGRAM_ITEM_HASH
        for m in (await response.json())["messages"]
    )

    # Single-message endpoint.
    response = await ccn_api_client.get(MESSAGE_URI)
    assert response.status == 200, await response.text()
    result = await response.json()
    assert result["status"] == "processed"
    assert result["message"]["item_hash"] == VPROGRAM_ITEM_HASH
    assert result["message"]["content"]["verification"]["backend"] == "sev_snp"

    # Headers content format must not crash on the new type.
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgType": "V-PROGRAM", "contentFormat": "headers"}
    )
    assert response.status == 200, await response.text()
    headers_messages = (await response.json())["messages"]
    assert len(headers_messages) == 1
    assert headers_messages[0]["content"] == {"address": SENDER}


@pytest.mark.asyncio
async def test_vprogram_pending_display(
    ccn_api_client,
    fixture_vprogram_message,
):
    # Not processed yet: the message must be visible as pending.
    response = await ccn_api_client.get(MESSAGE_URI)
    assert response.status == 200, await response.text()
    result = await response.json()
    assert result["status"] == "pending"
    assert result["messages"][0]["item_hash"] == VPROGRAM_ITEM_HASH


@pytest.mark.asyncio
async def test_vprogram_rejected_display(
    ccn_api_client,
    session_factory,
    message_processor,
    fixture_vprogram_message,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # No credit balance: processing rejects the message, and the rejected
    # message is still visible through the single-message endpoint. The refs
    # are seeded so that the credit check is the one that rejects.
    with session_factory() as session:
        insert_vprogram_refs(session)
        insert_bundle_pin(session)
        session.commit()
    store_manifest(message_processor, json.dumps(MANIFEST).encode())

    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(MESSAGE_URI)
    assert response.status == 200, await response.text()
    result = await response.json()
    assert result["status"] == "rejected"
    assert result["error_code"] == ErrorCode.CREDIT_INSUFFICIENT.value
