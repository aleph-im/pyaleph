import asyncio
import os
import shutil
import sys
from pathlib import Path

import pymongo
import pytest
import pytest_asyncio
from configmanager import Config

import aleph.config
from aleph.config import get_defaults
from aleph.model import init_db
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.web import create_app

TEST_DB = "ccn_automated_tests"


# Add the helpers to the PYTHONPATH.
# Note: mark the "helpers" directory as a source directory to tell PyCharm
# about this trick and avoid IDE errors.
sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


def drop_db(db_name: str, config: Config):
    client = pymongo.MongoClient(config.mongodb.uri.value)
    client.drop_database(db_name)


@pytest_asyncio.fixture
async def test_db():
    """
    Initializes and cleans a MongoDB database dedicated to automated tests.
    """

    config = Config(schema=get_defaults())
    config.mongodb.database.value = TEST_DB

    drop_db(TEST_DB, config)
    init_db(config, ensure_indexes=True)

    from aleph.model import db

    yield db


@pytest.fixture
def mock_config(mocker):
    config = Config(aleph.config.get_defaults())
    # To test handle_new_storage
    config.storage.store_files.value = True

    # We set the global variable directly instead of patching it because of an issue
    # with mocker.patch. mocker.patch uses hasattr to determine the properties of
    # the mock, which does not work well with configmanager Config objects.
    aleph.config.app_config = config
    return config


@pytest_asyncio.fixture
async def test_storage_service(mock_config) -> StorageService:
    data_folder = Path("./data")

    # Delete files from previous runs
    if data_folder.is_dir():
        shutil.rmtree(data_folder)
    data_folder.mkdir(parents=True)

    storage_engine = FileSystemStorageEngine(folder=data_folder)
    ipfs_client = make_ipfs_client(mock_config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(storage_engine=storage_engine, ipfs_service=ipfs_service)
    return storage_service


@pytest_asyncio.fixture
async def ccn_api_client(mocker, aiohttp_client, mock_config):
    # Make aiohttp return the stack trace on 500 errors
    event_loop = asyncio.get_event_loop()
    event_loop.set_debug(True)

    app = create_app(debug=True)
    app["config"] = mock_config
    app["p2p_client"] = mocker.AsyncMock()
    app["storage_service"] = mocker.AsyncMock()
    client = await aiohttp_client(app)

    return client
