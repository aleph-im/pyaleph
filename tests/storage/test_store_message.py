import json

import pytest

from aleph.exceptions import UnknownHashError
from aleph.handlers.storage import handle_new_storage
from aleph.schemas.pending_messages import parse_message, PendingStoreMessage
from aleph.storage import ContentSource, RawContent


@pytest.fixture
def fixture_message_file():
    message_dict = {
        "_id": {"$oid": "621908cb378bcd3ef596fa50"},
        "chain": "ETH",
        "item_hash": "7e4f914865028356704919810073ec5690ecc4bb0ee3bd6bdb24829fd532398f",
        "sender": "0x1772213F07b98eBf3e85CCf88Ac29482ff97d9B1",
        "type": "STORE",
        "channel": "TEST",
        "confirmed": True,
        "item_content": '{"address":"0x1772213F07b98eBf3e85CCf88Ac29482ff97d9B1","item_type":"ipfs","item_hash":"QmWxcFfKfmDddodV2iUKvkhGQ931AyakgkZRUNVPUq9E6G","time":1645807812.6829665,"id":1301,"name":"Amphibian Outlaws #1301","description":"Amphibian Outlaws is a collection of 7777 NFTs living together in Hyfall City. Each NFT is either a member of a mob family or a street gang. Visit https://www.amphibianoutlaws.com/ for more info.","attributes":[{"trait_type":"Type","value":"Typical"},{"trait_type":"Color","value":"Charcoal"},{"trait_type":"Eye Color","value":"Orange"},{"trait_type":"Hat","value":"Fedora"},{"trait_type":"Hat Color","value":"White & Blue"},{"trait_type":"Background","value":"Purple Fog"},{"trait_type":"Shades","value":"Black"},{"trait_type":"Cigar","value":"Red"}],"ref":"0xd55316fc244c7f5b44DC246e725c1C6c3E0cB8C2"}',
        "item_type": "inline",
        "signature": "0xc4d2660f8cd40f93dbfe153c67ebbdc86113811bf04fb1ce903a3da5da9017f011001587eee18837d2089a84589ae8be7df428d4b72f0a62e956868aef5938c61b",
        "size": 823,
        "time": 1645807812.6829786,
        "confirmations": [
            {
                "chain": "ETH",
                "height": 14276536,
                "hash": "0x28fd852984b1f2222ca1870a97f44cc34b535a49d2618f5689a10a67985935d5",
            }
        ],
    }
    return parse_message(message_dict)


@pytest.fixture
def fixture_message_directory():
    message_dict = {
        "_id": {"$oid": "1234"},
        "chain": "ETH",
        "item_hash": "b3d17833bcefb7a6eb2d9fa7c77cca3eed3a3fa901a904d35c529a71be25fc6d",
        "sender": "0xdeadbeef",
        "type": "STORE",
        "channel": "PINNING",
        "confirmed": False,
        "item_content": '{"address":"0x2278d6A697B2Be8aE4Ddf090f918d1642Ee43c8C","item_type":"ipfs","item_hash":"QmPZrod87ceK4yVvXQzRexDcuDgmLxBiNJ1ajLjLoMx9sU","time":1644409598.782}',
        "item_type": "inline",
        "signature": "0x755cce871af0ba577a940c2515f361b52726fb9c9c5a4c4a8323b9e773ca3008527f4ca7a73e3ea12c7df05b22b3e5c4fb27cfc9220b0cfcf2620c0f4d22c51d1c",
        "size": 158,
        "time": 1644409598.782,
    }
    return parse_message(message_dict)


@pytest.mark.asyncio
async def test_handle_new_storage_invalid_content(
    mock_config, fixture_message_directory: PendingStoreMessage
):
    missing_item_hash_content = {
        "address": "0x2278d6A697B2Be8aE4Ddf090f918d1642Ee43c8C",
        "item_type": "ipfs",
        "time": 1644409598.782,
    }

    result = await handle_new_storage(
        fixture_message_directory, missing_item_hash_content
    )
    assert result == -1

    missing_item_type_content = {
        "address": "0x2278d6A697B2Be8aE4Ddf090f918d1642Ee43c8C",
        "item_hash": "QmPZrod87ceK4yVvXQzRexDcuDgmLxBiNJ1ajLjLoMx9sU",
        "time": 1644409598.782,
    }

    result = await handle_new_storage(
        fixture_message_directory, missing_item_type_content
    )
    assert result == -1

    result = await handle_new_storage(fixture_message_directory, content={})
    assert result == -1


@pytest.mark.asyncio
async def test_handle_new_storage_file(
    mocker, mock_config, fixture_message_file: PendingStoreMessage
):
    content = json.loads(fixture_message_file.item_content)

    raw_content = RawContent(
        hash=content["item_hash"],
        source=ContentSource.IPFS,
        value=b"alea jacta est",
    )
    get_hash_content_mock = mocker.patch(
        "aleph.handlers.storage.get_hash_content", return_value=raw_content
    )
    mock_ipfs_api = mocker.MagicMock()
    ipfs_stats = {
        "Hash": "QmWxcFfKfmDddodV2iUKvkhGQ931AyakgkZRUNVPUq9E6G",
        "Size": 141863,
        "CumulativeSize": 141877,
        "Blocks": 0,
        "Type": "file",
    }
    mock_ipfs_api.files.stat = mocker.AsyncMock(return_value=ipfs_stats)
    mocker.patch("aleph.handlers.storage.get_ipfs_api", return_value=mock_ipfs_api)

    result = await handle_new_storage(fixture_message_file, content)
    assert result and result != -1

    # The IPFS stats are not added for files
    assert "engine_info" not in content
    assert content["size"] == len(raw_content)
    assert content["content_type"] == "file"

    assert get_hash_content_mock.called_once


@pytest.mark.asyncio
async def test_handle_new_storage_directory(
    mocker, mock_config, fixture_message_directory: PendingStoreMessage
):
    get_hash_content_mock = mocker.patch("aleph.handlers.storage.get_hash_content")
    mock_ipfs_api = mocker.MagicMock()
    ipfs_stats = {
        "Hash": "QmPZrod87ceK4yVvXQzRexDcuDgmLxBiNJ1ajLjLoMx9sU",
        "Size": 0,
        "CumulativeSize": 4560,
        "Blocks": 2,
        "Type": "directory",
    }
    mock_ipfs_api.files.stat = mocker.AsyncMock(return_value=ipfs_stats)
    mocker.patch("aleph.handlers.storage.get_ipfs_api", return_value=mock_ipfs_api)

    content = json.loads(fixture_message_directory.item_content)

    result = await handle_new_storage(fixture_message_directory, content)
    assert result and result != -1

    # Check the updates to the content dict
    assert content["engine_info"] == ipfs_stats
    assert content["size"] == ipfs_stats["CumulativeSize"]
    assert content["content_type"] == "directory"

    assert not get_hash_content_mock.called


@pytest.mark.asyncio
async def test_handle_new_storage_invalid_hash(
    mock_config, fixture_message_file: PendingStoreMessage
):
    content = json.loads(fixture_message_file.item_content)
    content["item_hash"] = "some-invalid-hash"

    with pytest.raises(UnknownHashError):
        _ = await handle_new_storage(fixture_message_file, content)
