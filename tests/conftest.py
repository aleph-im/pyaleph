import asyncio
import contextlib
import logging
import os
import shutil
import sys
from pathlib import Path

import alembic.command
import alembic.config
import pytest
import pytest_asyncio
from configmanager import Config

import aleph.config
from aleph.db.connection import make_engine, make_session_factory, make_db_url
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory
from aleph.web import create_aiohttp_app

# Add the helpers to the PYTHONPATH.
# Note: mark the "helpers" directory as a source directory to tell PyCharm
# about this trick and avoid IDE errors.
sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


@contextlib.contextmanager
def change_dir(directory: Path):
    current_directory = Path.cwd()
    try:
        os.chdir(directory)
        yield
    finally:
        os.chdir(current_directory)


def run_db_migrations(config: Config):
    logging.basicConfig(level=logging.DEBUG)

    project_dir = Path(__file__).parent.parent

    db_url = make_db_url(driver="psycopg2", config=config)
    alembic_cfg = alembic.config.Config("alembic.ini")
    alembic_cfg.attributes["configure_logger"] = False
    logging.getLogger("alembic").setLevel(logging.CRITICAL)

    with change_dir(project_dir):
        alembic.command.upgrade(alembic_cfg, "head", tag=db_url)


@pytest.fixture
def session_factory(mock_config):
    engine = make_engine(config=mock_config, echo=False, application_name="aleph-tests")

    with engine.begin() as conn:
        conn.execute("drop schema public cascade")
        conn.execute("create schema public")

    run_db_migrations(config=mock_config)
    return make_session_factory(engine)


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
async def node_cache(mock_config: Config):
    return NodeCache(
        redis_host=mock_config.redis.host.value, redis_port=mock_config.redis.port.value
    )


@pytest_asyncio.fixture
async def test_storage_service(mock_config: Config, mocker) -> StorageService:
    data_folder = Path("./data")

    # Delete files from previous runs
    if data_folder.is_dir():
        shutil.rmtree(data_folder)
    data_folder.mkdir(parents=True)

    storage_engine = FileSystemStorageEngine(folder=data_folder)
    ipfs_client = make_ipfs_client(mock_config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=storage_engine,
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    return storage_service


@pytest_asyncio.fixture
async def ccn_api_client(
    mocker, aiohttp_client, mock_config, session_factory: DbSessionFactory
):
    # Make aiohttp return the stack trace on 500 errors
    event_loop = asyncio.get_event_loop()
    event_loop.set_debug(True)

    app = create_aiohttp_app(debug=True)
    app["config"] = mock_config
    app["p2p_client"] = mocker.AsyncMock()
    app["storage_service"] = mocker.AsyncMock()
    app["session_factory"] = session_factory
    client = await aiohttp_client(app)

    return client
