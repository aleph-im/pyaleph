import pymongo
import pytest
import pytest_asyncio
from configmanager import Config

import aleph.config
from aleph.config import get_defaults
from aleph.model import init_db

TEST_DB = "ccn_automated_tests"


@pytest.fixture
def mock_config():
    config = Config(aleph.config.get_defaults())
    # To test handle_new_storage
    config.storage.store_files.value = True

    # We set the global variable directly instead of patching it because of an issue
    # with mocker.patch. mocker.patch uses hasattr to determine the properties of
    # the mock, which does not work well with configmanager Config objects.
    aleph.config.app_config = config
    return config


def drop_db(db_name: str, config: Config):
    client = pymongo.MongoClient(config.mongodb.uri.value)
    client.drop_database(db_name)


@pytest_asyncio.fixture
async def test_db(mock_config):
    """
    Initializes and cleans a MongoDB database dedicated to automated tests.
    """

    mock_config.mongodb.database.value = TEST_DB

    drop_db(TEST_DB, mock_config)
    init_db(mock_config, ensure_indexes=True)

    from aleph.model import db

    yield db
