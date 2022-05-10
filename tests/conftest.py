import asyncio
import os

import pymongo
import pytest_asyncio
from configmanager import Config

from aleph.config import get_defaults
from aleph.model import init_db
from aleph.web import create_app

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


@pytest_asyncio.fixture
async def ccn_api_client(aiohttp_client):
    # Make aiohttp return the stack trace on 500 errors
    event_loop = asyncio.get_event_loop()
    event_loop.set_debug(True)

    app = create_app()
    client = await aiohttp_client(app)

    return client
