import json
from typing import Any, Mapping

import pytest
from configmanager import Config
from message_test_helpers import make_validated_message_from_dict
from sqlalchemy import select

from aleph.db.models import MessageDb, StoredFileDb
from aleph.handlers.content.store import StoreMessageHandler
from aleph.schemas.message_content import ContentSource, RawContent
from aleph.services.ipfs import IpfsService
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType


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
    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=IpfsService(ipfs_client=mock_ipfs_client),
        node_cache=mocker.AsyncMock(),
    )
    storage_service.get_hash_content = get_hash_content_mock = mocker.AsyncMock(return_value=raw_content)  # type: ignore
    store_message_handler = StoreMessageHandler(
        storage_service=storage_service, grace_period=24
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

    storage_service = StorageService(
        storage_engine=storage_engine,
        ipfs_service=IpfsService(ipfs_client=mock_ipfs_client),
        node_cache=mocker.AsyncMock(),
    )
    store_message_handler = StoreMessageHandler(
        storage_service=storage_service, grace_period=24
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

    storage_engine.assert_not_called()
