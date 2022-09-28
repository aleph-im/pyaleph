from logging import getLogger

from configmanager import Config

from aleph.model.filepin import PermanentPin

from pymongo import MongoClient

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket

LOGGER = getLogger("model")

db_backend = None

# Mongodb connection and db
connection = None
db = None


def init_db_globals(config: Config):
    global connection, db
    connection = AsyncIOMotorClient(config.mongodb.uri.value, tz_aware=True)
    db = connection[config.mongodb.database.value]


def make_gridfs_client():
    global db
    if db is None:
        raise ValueError("DB is not initialized")

    return AsyncIOMotorGridFSBucket(db)


def init_db(config: Config, ensure_indexes: bool = True):
    init_db_globals(config)
    sync_connection: MongoClient = MongoClient(config.mongodb.uri.value, tz_aware=True)
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
