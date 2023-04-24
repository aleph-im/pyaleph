import base64
from typing import Any

import aiohttp
import orjson
import pytest
from aleph_message.models import ItemHash

from aleph.db.accessors.files import get_file
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from in_memory_storage_engine import InMemoryStorageEngine

IPFS_ADD_FILE_URI = "/api/v0/ipfs/add_file"
IPFS_ADD_JSON_URI = "/api/v0/ipfs/add_json"
STORAGE_ADD_FILE_URI = "/api/v0/storage/add_file"
STORAGE_ADD_JSON_URI = "/api/v0/storage/add_json"

GET_STORAGE_URI = "/api/v0/storage"
GET_STORAGE_RAW_URI = "/api/v0/storage/raw"

FILE_CONTENT = b"Hello earthlings, I come in pieces"
EXPECTED_FILE_SHA256 = (
    "bb6e53f2738e5934b9a2125a9dc3d76211720e5152bdbcd4b236363d18d4f8a3"
)
EXPECTED_FILE_CID = "QmPoBEaYRf2HDHHFsD7tYkCcSdpLbx5CYDgCgDtW4ywhSK"

JSON_CONTENT = {"first name": "Jay", "last_name": "Son"}
EXPECTED_JSON_FILE_SHA256 = (
    "b7c7b2db0bcec890b8c859b2b76e7c998de15e31ccc945bc7425c4bdc091a0b2"
)


@pytest.fixture
def api_client(ccn_api_client, mocker):
    ipfs_service = mocker.AsyncMock()
    ipfs_service.add_bytes = mocker.AsyncMock(return_value=EXPECTED_FILE_CID)
    ipfs_service.add_file = mocker.AsyncMock(
        return_value={
            "Hash": EXPECTED_FILE_CID,
            "Size": 34,
            "Name": "hello-earthlings.txt",
        }
    )
    ipfs_service.get_ipfs_content = mocker.AsyncMock(return_value=FILE_CONTENT)

    ccn_api_client.app["storage_service"] = StorageService(
        storage_engine=InMemoryStorageEngine(files={}),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    return ccn_api_client


async def add_file(
    api_client,
    session_factory: DbSessionFactory,
    uri: str,
    file_content: bytes,
    expected_file_hash: str,
):
    form_data = aiohttp.FormData()
    form_data.add_field("file", file_content)

    post_response = await api_client.post(uri, data=form_data)
    assert post_response.status == 200, await post_response.text()
    post_response_json = await post_response.json()
    assert post_response_json["status"] == "success"
    file_hash = post_response_json["hash"]
    assert file_hash == expected_file_hash

    # Assert that the file is downloadable
    get_file_response = await api_client.get(f"{GET_STORAGE_RAW_URI}/{file_hash}")
    assert get_file_response.status == 200
    response_data = await get_file_response.read()

    # Check that the file appears in the DB
    with session_factory() as session:
        file = get_file(session=session, file_hash=file_hash)
        assert file is not None
        assert file.hash == file_hash
        assert file.type == FileType.FILE
        assert file.size == len(file_content)

    assert response_data == file_content


@pytest.mark.asyncio
async def test_storage_add_file(api_client, session_factory: DbSessionFactory):
    await add_file(
        api_client,
        session_factory,
        uri=STORAGE_ADD_FILE_URI,
        file_content=FILE_CONTENT,
        expected_file_hash=EXPECTED_FILE_SHA256,
    )


@pytest.mark.asyncio
async def test_ipfs_add_file(api_client, session_factory: DbSessionFactory):
    await add_file(
        api_client,
        session_factory,
        uri=IPFS_ADD_FILE_URI,
        file_content=FILE_CONTENT,
        expected_file_hash=EXPECTED_FILE_CID,
    )


async def add_json(
    api_client,
    session_factory: DbSessionFactory,
    uri: str,
    json: Any,
    expected_file_hash: ItemHash,
):
    serialized_json = orjson.dumps(json)

    post_response = await api_client.post(uri, json=json)
    assert post_response.status == 200, await post_response.text()
    post_response_json = await post_response.json()
    assert post_response_json["status"] == "success"
    file_hash = post_response_json["hash"]
    assert file_hash == expected_file_hash

    # Assert that the JSON content is gettable
    get_json_response = await api_client.get(f"{GET_STORAGE_URI}/{file_hash}")
    assert get_json_response.status == 200

    response_json = await get_json_response.json()
    assert response_json["status"] == "success"
    assert response_json["hash"] == file_hash
    assert response_json["engine"] == expected_file_hash.item_type.value
    assert base64.b64decode(response_json["content"]) == serialized_json

    # Assert that the corresponding file is downloadable
    get_file_response = await api_client.get(f"{GET_STORAGE_RAW_URI}/{file_hash}")
    assert get_file_response.status == 200
    assert await get_file_response.read() == serialized_json

    # Check that the file appears in the DB
    with session_factory() as session:
        file = get_file(session=session, file_hash=file_hash)
        assert file is not None
        assert file.hash == file_hash
        assert file.type == FileType.FILE

    # Ch


@pytest.mark.asyncio
async def test_storage_add_json(api_client, session_factory: DbSessionFactory):
    await add_json(
        api_client,
        session_factory,
        uri=STORAGE_ADD_JSON_URI,
        json=JSON_CONTENT,
        expected_file_hash=ItemHash(EXPECTED_JSON_FILE_SHA256),
    )


@pytest.mark.asyncio
async def test_ipfs_add_json(api_client, session_factory: DbSessionFactory):
    await add_json(
        api_client,
        session_factory,
        uri=IPFS_ADD_JSON_URI,
        json=JSON_CONTENT,
        # Note: we mock the call to the IPFS daemon, so we reuse the same CID to avoid
        # creating a second fixture.
        expected_file_hash=ItemHash(EXPECTED_FILE_CID),
    )
