import json
from typing import Any, Mapping
from unittest.mock import ANY

import pytest
from aleph_message.models import ItemType
from configmanager import Config
from message_test_helpers import make_validated_message_from_dict
from sqlalchemy import select

from aleph.db.models import MessageDb, StoredFileDb
from aleph.handlers.content.store import StoreMessageHandler, _apply_fetch_jitter
from aleph.schemas.message_content import ContentSource, RawContent
from aleph.services.ipfs import IpfsService
from aleph.storage import StorageService
from aleph.toolkit.constants import DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE
from aleph.toolkit.metrics_keys import (
    STORE_FETCH_IPFS_DURATION_MS_SUM_KEY,
    STORE_FETCH_IPFS_FAILED_KEY,
    STORE_FETCH_IPFS_TOTAL_KEY,
    STORE_FETCH_STORAGE_DURATION_MS_SUM_KEY,
    STORE_FETCH_STORAGE_FAILED_KEY,
    STORE_FETCH_STORAGE_TOTAL_KEY,
    store_fetch_keys,
)
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType


@pytest.mark.asyncio
async def test_apply_fetch_jitter_skipped_when_zero(mocker):
    """fetch_jitter_seconds=0 disables jitter and does not sleep."""
    sleep_mock = mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    await _apply_fetch_jitter(0, "abc")
    sleep_mock.assert_not_called()


@pytest.mark.asyncio
async def test_apply_fetch_jitter_sleeps_within_window(mocker):
    """fetch_jitter_seconds>0 sleeps for a delay drawn from [0, window]."""
    sleep_mock = mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    await _apply_fetch_jitter(30.0, "abc")
    sleep_mock.assert_called_once()
    delay = sleep_mock.call_args.args[0]
    assert 0.0 <= delay <= 30.0


def test_store_fetch_keys_differentiate_by_type():
    """Metric keys are split by item type so ipfs and storage fetches never
    share a counter."""
    ipfs_keys = store_fetch_keys(ItemType.ipfs)
    storage_keys = store_fetch_keys(ItemType.storage)

    assert ipfs_keys == (
        STORE_FETCH_IPFS_TOTAL_KEY,
        STORE_FETCH_IPFS_FAILED_KEY,
        STORE_FETCH_IPFS_DURATION_MS_SUM_KEY,
    )
    assert storage_keys == (
        STORE_FETCH_STORAGE_TOTAL_KEY,
        STORE_FETCH_STORAGE_FAILED_KEY,
        STORE_FETCH_STORAGE_DURATION_MS_SUM_KEY,
    )
    assert set(ipfs_keys).isdisjoint(storage_keys)


@pytest.fixture
def fixture_message_file() -> MessageDb:
    message_dict: Mapping[str, Any] = {
        "chain": "ETH",
        "item_hash": "7e4f914865028356704919810073ec5690ecc4bb0ee3bd6bdb24829fd532398f",
        "sender": "0x1772213F07b98eBf3e85CCf88Ac29482ff97d9B1",
        "type": "STORE",
        "channel": "TEST",
        "item_content": '{"address":"0x1772213F07b98eBf3e85CCf88Ac29482ff97d9B1","item_type":"ipfs","item_hash":"QmWxcFfKfmDddodV2iUKvkhGQ931AyakgkZRUNVPUq9E6G","time":1645807812.6829665,"id":1301,"name":"Amphibian Outlaws #1301","description":"Amphibian Outlaws is a collection of 7777 NFTs living together in Hyfall City. Each NFT is either a member of a mob family or a street gang. Visit https://www.amphibianoutlaws.com/ for more info.","attributes":[{"trait_type":"Type","value":"Typical"},{"trait_type":"Color","value":"Charcoal"},{"trait_type":"Eye Color","value":"Orange"},{"trait_type":"Hat","value":"Fedora"},{"trait_type":"Hat Color","value":"White & Blue"},{"trait_type":"Background","value":"Purple Fog"},{"trait_type":"Shades","value":"Black"},{"trait_type":"Cigar","value":"Red"}],"ref":"0xd55316fc244c7f5b44DC246e725c1C6c3E0cB8C2"}',
        "item_type": "inline",
        "signature": "0xc4d2660f8cd40f93dbfe153c67ebbdc86113811bf04fb1ce903a3da5da9017f011001587eee18837d2089a84589ae8be7df428d4b72f0a62e956868aef5938c61b",
        "time": 1645807812.6829786,
    }
    return make_validated_message_from_dict(
        message_dict,
        raw_content=message_dict["item_content"],
    )


@pytest.fixture
def fixture_message_directory() -> MessageDb:
    message_dict: Mapping[str, Any] = {
        "chain": "ETH",
        "item_hash": "b3d17833bcefb7a6eb2d9fa7c77cca3eed3a3fa901a904d35c529a71be25fc6d",
        "sender": "0xdeadbeef",
        "type": "STORE",
        "channel": "PINNING",
        "item_content": '{"address":"0x2278d6A697B2Be8aE4Ddf090f918d1642Ee43c8C","item_type":"ipfs","item_hash":"QmPZrod87ceK4yVvXQzRexDcuDgmLxBiNJ1ajLjLoMx9sU","time":1644409598.782}',
        "item_type": "inline",
        "signature": "0x755cce871af0ba577a940c2515f361b52726fb9c9c5a4c4a8323b9e773ca3008527f4ca7a73e3ea12c7df05b22b3e5c4fb27cfc9220b0cfcf2620c0f4d22c51d1c",
        "time": 1644409598.782,
    }

    return make_validated_message_from_dict(
        message_dict, raw_content=message_dict["item_content"]
    )


@pytest.mark.asyncio
async def test_handle_new_storage_file(
    mocker,
    session_factory: DbSessionFactory,
    mock_config: Config,
    fixture_message_file: MessageDb,
):
    assert fixture_message_file.item_content is not None  # for mypy
    content = json.loads(fixture_message_file.item_content)

    raw_content = RawContent(
        hash=content["item_hash"],
        source=ContentSource.IPFS,
        value=b"alea jacta est",
    )
    mock_ipfs_client = mocker.MagicMock()
    ipfs_stats = {
        "Hash": "QmWxcFfKfmDddodV2iUKvkhGQ931AyakgkZRUNVPUq9E6G",
        "Size": 141863,
        "CumulativeSize": 141877,
        "Blocks": 0,
        "Type": "file",
    }
    mock_ipfs_client.files.stat = mocker.AsyncMock(return_value=ipfs_stats)

    message = fixture_message_file
    node_cache = mocker.AsyncMock()
    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=IpfsService(ipfs_client=mock_ipfs_client),
        node_cache=node_cache,
    )
    get_hash_content_mock = mocker.AsyncMock(return_value=raw_content)
    storage_service.get_hash_content = get_hash_content_mock  # type: ignore[method-assign]
    store_message_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )
    with session_factory() as session:
        await store_message_handler.fetch_related_content(
            session=session, message=message
        )
        session.commit()

    with session_factory() as session:
        stored_files = list((session.execute(select(StoredFileDb))).scalars())

    assert len(stored_files) == 1
    stored_file: StoredFileDb = stored_files[0]

    assert stored_file.hash == content["item_hash"]
    assert stored_file.type == FileType.FILE
    assert stored_file.size == len(raw_content)

    get_hash_content_mock.assert_called_once()

    # ipfs item via http path: ipfs counters incremented, duration recorded, no failure
    node_cache.incr.assert_any_call(STORE_FETCH_IPFS_TOTAL_KEY)
    node_cache.incrby.assert_any_call(STORE_FETCH_IPFS_DURATION_MS_SUM_KEY, ANY)
    assert (
        mocker.call(STORE_FETCH_IPFS_FAILED_KEY) not in node_cache.incr.call_args_list
    )


@pytest.mark.asyncio
async def test_handle_new_storage_directory(
    mocker,
    session_factory: DbSessionFactory,
    mock_config: Config,
    fixture_message_directory: MessageDb,
):
    mock_ipfs_client = mocker.MagicMock()
    ipfs_stats = {
        "Hash": "QmPZrod87ceK4yVvXQzRexDcuDgmLxBiNJ1ajLjLoMx9sU",
        "Size": 0,
        "CumulativeSize": 4560,
        "Blocks": 2,
        "Type": "directory",
    }
    mock_ipfs_client.files.stat = mocker.AsyncMock(return_value=ipfs_stats)

    message = fixture_message_directory
    storage_engine = mocker.AsyncMock()

    node_cache = mocker.AsyncMock()
    storage_service = StorageService(
        storage_engine=storage_engine,
        ipfs_service=IpfsService(ipfs_client=mock_ipfs_client),
        node_cache=node_cache,
    )
    store_message_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )

    with session_factory() as session:
        await store_message_handler.fetch_related_content(
            session=session, message=message
        )
        session.commit()

    with session_factory() as session:
        stored_files = list((session.execute(select(StoredFileDb))).scalars())

    assert len(stored_files) == 1
    stored_file = stored_files[0]

    # Check the updates to the message content
    assert stored_file.hash == ipfs_stats["Hash"]
    assert stored_file.size == ipfs_stats["CumulativeSize"]
    assert stored_file.type == FileType.DIRECTORY

    assert not storage_engine.called

    # pin path (ipfs): ipfs counters incremented, duration recorded, no failure
    node_cache.incr.assert_any_call(STORE_FETCH_IPFS_TOTAL_KEY)
    node_cache.incrby.assert_any_call(STORE_FETCH_IPFS_DURATION_MS_SUM_KEY, ANY)
    assert (
        mocker.call(STORE_FETCH_IPFS_FAILED_KEY) not in node_cache.incr.call_args_list
    )


@pytest.mark.asyncio
async def test_handle_storage_fetch_failure_metrics(
    mocker,
    session_factory: DbSessionFactory,
    mock_config: Config,
    fixture_message_directory: MessageDb,
):
    """A failed pin increments the failure counter, records no duration, and
    re-raises so the message is retried."""
    mock_ipfs_client = mocker.MagicMock()
    mock_ipfs_client.files.stat = mocker.AsyncMock(
        return_value={
            "Hash": "QmPZrod87ceK4yVvXQzRexDcuDgmLxBiNJ1ajLjLoMx9sU",
            "Size": 0,
            "CumulativeSize": 4560,
            "Blocks": 2,
            "Type": "directory",
        }
    )

    node_cache = mocker.AsyncMock()
    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=IpfsService(ipfs_client=mock_ipfs_client),
        node_cache=node_cache,
    )
    # A non-APIError exception confirms the broad except counts every failure mode.
    mocker.patch.object(
        storage_service.ipfs_service,
        "pin_add",
        side_effect=RuntimeError("pin failed"),
    )
    store_message_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )

    with session_factory() as session:
        with pytest.raises(RuntimeError, match="pin failed"):
            await store_message_handler.fetch_related_content(
                session=session, message=fixture_message_directory
            )

    node_cache.incr.assert_any_call(STORE_FETCH_IPFS_TOTAL_KEY)
    node_cache.incr.assert_any_call(STORE_FETCH_IPFS_FAILED_KEY)
    node_cache.incrby.assert_not_called()


@pytest.mark.asyncio
async def test_handle_new_storage_type_file_uses_storage_counters(
    mocker,
    session_factory: DbSessionFactory,
    mock_config: Config,
):
    """A storage-type STORE file goes through the http path and increments the
    storage counters, leaving the ipfs counters untouched."""
    file_hash = "315f7313eb97d2c8299e3ee9c19d81f226c44ccf81c387c9fb25c54fced245f5"
    message_dict: Mapping[str, Any] = {
        "chain": "ETH",
        "item_hash": "7e4f914865028356704919810073ec5690ecc4bb0ee3bd6bdb24829fd532398f",
        "sender": "0xdeadbeef",
        "type": "STORE",
        "channel": "TEST",
        "item_content": json.dumps(
            {
                "address": "0xdeadbeef",
                "item_type": "storage",
                "item_hash": file_hash,
                "time": 1645807812.6829665,
            }
        ),
        "item_type": "inline",
        "signature": "0xfake",
        "time": 1645807812.6829786,
    }
    message = make_validated_message_from_dict(
        message_dict, raw_content=message_dict["item_content"]
    )

    raw_content = RawContent(
        hash=file_hash, source=ContentSource.P2P, value=b"storage bytes"
    )
    node_cache = mocker.AsyncMock()
    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=mocker.AsyncMock(),
        node_cache=node_cache,
    )
    get_hash_content_mock = mocker.AsyncMock(return_value=raw_content)
    storage_service.get_hash_content = get_hash_content_mock  # type: ignore[method-assign]
    store_message_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )

    with session_factory() as session:
        await store_message_handler.fetch_related_content(
            session=session, message=message
        )
        session.commit()

    get_hash_content_mock.assert_called_once()
    node_cache.incr.assert_any_call(STORE_FETCH_STORAGE_TOTAL_KEY)
    node_cache.incrby.assert_any_call(STORE_FETCH_STORAGE_DURATION_MS_SUM_KEY, ANY)
    assert mocker.call(STORE_FETCH_IPFS_TOTAL_KEY) not in node_cache.incr.call_args_list


@pytest.mark.asyncio
async def test_store_files_is_false(
    mocker,
    session_factory: DbSessionFactory,
    mock_config: Config,
    fixture_message_directory: MessageDb,
):
    mock_ipfs_client = mocker.MagicMock()
    ipfs_stats = {
        "Hash": "QmPZrod87ceK4yVvXQzRexDcuDgmLxBiNJ1ajLjLoMx9sU",
        "Size": 42,
        "CumulativeSize": 4560,
        "Blocks": 2,
        "Type": "file",
    }
    mock_ipfs_client.files.stat = mocker.AsyncMock(return_value=ipfs_stats)

    mock_config.storage.store_files.value = False

    message = fixture_message_directory
    storage_engine = mocker.AsyncMock()

    storage_service = StorageService(
        storage_engine=storage_engine,
        ipfs_service=IpfsService(ipfs_client=mock_ipfs_client),
        node_cache=mocker.AsyncMock(),
    )
    _get_hash_content_mock = mocker.patch.object(storage_service, "get_hash_content")
    store_message_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )

    with session_factory() as session:
        await store_message_handler.fetch_related_content(
            session=session, message=message
        )
        session.commit()

    with session_factory() as session:
        stored_files = list((session.execute(select(StoredFileDb))).scalars())

    assert len(stored_files) == 1
    stored_file = stored_files[0]

    # Check the updates to the message content
    assert stored_file.hash == ipfs_stats["Hash"]
    assert stored_file.type == FileType.FILE


@pytest.mark.asyncio
async def test_store_files_is_false_ipfs_is_disabled(
    mocker,
    session_factory: DbSessionFactory,
    mock_config: Config,
    fixture_message_directory: MessageDb,
):
    mock_ipfs_client = mocker.MagicMock()
    ipfs_stats = {
        "Hash": "QmPZrod87ceK4yVvXQzRexDcuDgmLxBiNJ1ajLjLoMx9sU",
        "Size": 42,
        "CumulativeSize": 4560,
        "Blocks": 2,
        "Type": "file",
    }
    mock_ipfs_client.files.stat = mocker.AsyncMock(return_value=ipfs_stats)

    mock_config.storage.store_files.value = False
    mock_config.ipfs.enabled.value = False

    message = fixture_message_directory
    storage_engine = mocker.AsyncMock()

    storage_service = StorageService(
        storage_engine=storage_engine,
        ipfs_service=IpfsService(ipfs_client=mock_ipfs_client),
        node_cache=mocker.AsyncMock(),
    )
    _get_hash_content_mock = mocker.patch.object(storage_service, "get_hash_content")
    store_message_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )

    with session_factory() as session:
        await store_message_handler.fetch_related_content(
            session=session, message=message
        )
        session.commit()

    with session_factory() as session:
        stored_files = list((session.execute(select(StoredFileDb))).scalars())

    assert len(stored_files) == 1
    stored_file = stored_files[0]

    # Check the updates to the message content
    assert stored_file.hash == ipfs_stats["Hash"]
    assert stored_file.type == FileType.FILE
