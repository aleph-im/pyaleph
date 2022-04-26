import pytest
from aleph.model import init_db
from aleph.config import get_defaults
from configmanager import Config
import pymongo
import pytest_asyncio


TEST_DB = "ccn_automated_tests"


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
