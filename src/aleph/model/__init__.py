from logging import getLogger

log = getLogger(__name__)

import pymongo
try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    # Backward compatibility with PyMongo 2.2
    from pymongo import Connection as MongoClient

from motor.motor_asyncio import AsyncIOMotorClient

db_backend = None

# Mongodb connection and db
connection = None
db = None


def init_db(config, ensure_indexes=True):
    global connection, db
    connection = AsyncIOMotorClient(config.mongodb.uri.value,
                                    tz_aware=True)
    db = connection[config.mongodb.database.value]
    sync_connection = MongoClient(config.mongodb.uri.value,
                                    tz_aware=True)
    sync_db = sync_connection[config.mongodb.database.value]

    if ensure_indexes:
        from aleph.model.posts import Post
        Post.ensure_indexes(sync_db)
