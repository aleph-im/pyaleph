from logging import getLogger

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    # Backward compatibility with PyMongo 2.2
    from pymongo import Connection as MongoClient

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket

LOGGER = getLogger('model')

db_backend = None

# Mongodb connection and db
connection = None
db = None
fs = None


def init_db(config, ensure_indexes=True):
    global connection, db, fs
    connection = AsyncIOMotorClient(config.mongodb.uri.value,
                                    tz_aware=True)
    db = connection[config.mongodb.database.value]
    fs = AsyncIOMotorGridFSBucket(db)
    sync_connection = MongoClient(config.mongodb.uri.value,
                                  tz_aware=True)
    sync_db = sync_connection[config.mongodb.database.value]

    if ensure_indexes:
        LOGGER.info('Inserting indexes')
        from aleph.model.messages import Message
        Message.ensure_indexes(sync_db)
        from aleph.model.pending import PendingMessage, PendingTX
        PendingMessage.ensure_indexes(sync_db)
        PendingTX.ensure_indexes(sync_db)
        from aleph.model.chains import Chain
        Chain.ensure_indexes(sync_db)
        from aleph.model.p2p import Peer
        Peer.ensure_indexes(sync_db)
        # from aleph.model.hashes import Hash
        # Hash.ensure_indexes(sync_db)
