"""
This migration checks all the files stored in local storage (=GridFS) and compares them to the list
of messages already on the node. The files that are not linked to any message are scheduled for
deletion.
"""

import asyncio
import datetime as dt
import logging
from dataclasses import asdict
from typing import Optional, FrozenSet, Any, List

from aleph_message.models import MessageType
from configmanager import Config

import aleph.model
from aleph.config import get_defaults
from aleph.model import init_db_globals
from aleph.model.messages import Message
from aleph.model.scheduled_deletions import ScheduledDeletion, ScheduledDeletionInfo

logger = logging.getLogger()


async def async_upgrade(config_file: Optional[str], **kwargs):
    config = Config(schema=get_defaults())
    if config_file is not None:
        config.yaml.load(config_file)

    init_db_globals(config=config)
    collections = await aleph.model.db.list_collection_names()
    if ScheduledDeletion.COLLECTION in collections:
        logging.info(
            "%s collection is already present. Skipping migration.",
            ScheduledDeletion.COLLECTION,
        )
        return

    # Get a set of all the files currently in GridFS
    gridfs_files = frozenset(
        [
            file["filename"]
            async for file in aleph.model.db["fs.files"].find(
                projection={"filename": 1}, batch_size=1000
            )
        ]
    )

    print(len(gridfs_files))

    # Get all the messages that potentially store data in local storage:
    # * AGGREGATEs with item_type=="storage"
    # * POSTs with item_type=="storage"
    # * STOREs with content.item_type=="storage"
    async def get_hashes(
        msg_type: MessageType, item_type_field: str, item_hash_field: str
    ) -> FrozenSet[str]:
        def rgetitem(dictionary: Any, fields: List[str]) -> Any:
            value = dictionary[fields[0]]
            if len(fields) > 1:
                return rgetitem(value, fields[1:])
            return value

        return frozenset(
            [
                rgetitem(msg, item_hash_field.split("."))
                async for msg in Message.collection.find(
                    {"type": msg_type, item_type_field: "storage"},
                    {item_hash_field: 1},
                    batch_size=1000,
                )
            ]
        )

    aggregates = await get_hashes(MessageType.aggregate, "item_type", "item_hash")
    posts = await get_hashes(MessageType.post, "item_type", "item_hash")
    stores = await get_hashes(
        MessageType.store, "content.item_type", "content.item_hash"
    )

    files_to_preserve = aggregates | posts | stores
    files_to_delete = gridfs_files - files_to_preserve
    delete_by = dt.datetime.utcnow()

    await ScheduledDeletion.collection.insert_many(
        [
            asdict(ScheduledDeletionInfo(filename=file_to_delete, delete_by=delete_by))
            for file_to_delete in files_to_delete
        ]
    )


def upgrade(config_file: str, **kwargs):
    asyncio.run(async_upgrade(config_file=config_file, **kwargs))


def downgrade(**kwargs):
    # Nothing to do, processing the chain data multiple times only adds some load on the node.
    pass
