import asyncio
import os
import shutil
import sys
from pathlib import Path

import pytest
import pytest_asyncio
from configmanager import Config

import aleph.config
from aleph.db.connection import make_engine, make_session_factory
from aleph.db.models.base import Base
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory
from aleph.web import create_app

# Add the helpers to the PYTHONPATH.
# Note: mark the "helpers" directory as a source directory to tell PyCharm
# about this trick and avoid IDE errors.
sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


@pytest.fixture
def session_factory(mock_config):
    engine = make_engine(config=mock_config, echo=False, application_name="aleph-tests")

    with engine.begin() as conn:
        Base.metadata.drop_all(conn)
        # TODO: run migrations instead
        Base.metadata.create_all(conn)

        # Here go all the annoying patchworks that are required because we do not run
        # the migration scripts. Address the todo above and these can all disappear!

        # Aggregates are not described in SQLAlchemy, so we need to create them manually.
        conn.execute("DROP AGGREGATE IF EXISTS jsonb_merge(jsonb)")
        conn.execute(
            """
        CREATE AGGREGATE jsonb_merge(jsonb) (
            SFUNC = 'jsonb_concat',
            STYPE = jsonb,
            INITCOND = '{}'
        )"""
        )

        # Indexes with NULLS NOT DISTINCT are not yet supported in SQLA.
        conn.execute(
            "ALTER TABLE balances DROP CONSTRAINT balances_address_chain_dapp_uindex"
        )
        conn.execute(
            "ALTER TABLE balances ADD CONSTRAINT balances_address_chain_dapp_uindex UNIQUE NULLS NOT DISTINCT (address, chain, dapp)"
        )

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
async def test_storage_service(mock_config) -> StorageService:
    data_folder = Path("./data")

    # Delete files from previous runs
    if data_folder.is_dir():
        shutil.rmtree(data_folder)
    data_folder.mkdir(parents=True)

    storage_engine = FileSystemStorageEngine(folder=data_folder)
    ipfs_client = make_ipfs_client(mock_config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=storage_engine, ipfs_service=ipfs_service
    )
    return storage_service


@pytest_asyncio.fixture
async def ccn_api_client(
    mocker, aiohttp_client, mock_config, session_factory: DbSessionFactory
):
    # Make aiohttp return the stack trace on 500 errors
    event_loop = asyncio.get_event_loop()
    event_loop.set_debug(True)

    app = create_app(debug=True)
    app["config"] = mock_config
    app["p2p_client"] = mocker.AsyncMock()
    app["storage_service"] = mocker.AsyncMock()
    app["session_factory"] = session_factory
    client = await aiohttp_client(app)

    return client
