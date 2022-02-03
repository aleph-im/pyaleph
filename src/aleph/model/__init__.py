from logging import getLogger

from configmanager import Config

from aleph.model.filepin import PermanentPin

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    # Backward compatibility with PyMongo 2.2
    from pymongo import Connection as MongoClient

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket

LOGGER = getLogger("model")

db_backend = None

# Mongodb connection and db
connection = None
db = None
fs = None


def init_db(config: Config, ensure_indexes: bool = True):
    global connection, db, fs
    connection = AsyncIOMotorClient(config.mongodb.uri.value, tz_aware=True)
    db = connection[config.mongodb.database.value]
    fs = AsyncIOMotorGridFSBucket(db)
    sync_connection = MongoClient(config.mongodb.uri.value, tz_aware=True)
    sync_db = sync_connection[config.mongodb.database.value]

    from aleph.model.messages import CappedMessage

    CappedMessage.create(sync_db)

    if ensure_indexes:
        LOGGER.info("Inserting indexes")
        from aleph.model.messages import Message

        Message.ensure_indexes(sync_db)
        from aleph.model.pending import PendingMessage, PendingTX

        PendingMessage.ensure_indexes(sync_db)
        PendingTX.ensure_indexes(sync_db)
        from aleph.model.chains import Chain

        Chain.ensure_indexes(sync_db)
        from aleph.model.p2p import Peer

        Peer.ensure_indexes(sync_db)

        PermanentPin.ensure_indexes(sync_db)

    from aleph.model.messages import Message

    Message.fix_message_confirmations(sync_db)
