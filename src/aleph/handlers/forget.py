import logging
from typing import Dict

from aioipfs.api import RepoAPI
from aioipfs.exceptions import NotPinnedError
from aleph_message.models import ForgetMessage, ItemType, MessageType, StoreContent

from aleph.model.filepin import PermanentPin
from aleph.model.hashes import delete_value
from aleph.model.messages import Message
from aleph.services.ipfs.common import get_ipfs_api
from aleph.storage import get_message_content
from aleph.utils import item_type_from_hash

logger = logging.getLogger(__name__)


async def count_file_references(storage_hash: str) -> int:
    """Count the number of references to a file on Aleph."""
    logger.debug(f"Counting documents for {storage_hash}")
    return await Message.collection.count_documents(
        filter={"content.item_hash": storage_hash},
    )


async def file_references_exist(storage_hash: str) -> bool:
    """Check if references to a file on Aleph exist.
    """
    return bool(await Message.collection.count_documents(
        filter={"content.item_hash": storage_hash}, limit=1))


async def garbage_collect(storage_hash: str, storage_type: ItemType):
    """If a file does not have any reference left, delete or unpin it.

    This is typically called after 'forgetting' a message.
    """
    logger.debug(f"Garbage collecting {storage_hash}")

    if await PermanentPin.collection.count_documents(filter={"multihash": storage_hash}, limit=1) > 0:
        logger.debug(f"Permanent pin will not be collected {storage_hash}")
        return

    if await file_references_exist(storage_hash):
        logger.debug(f"File {storage_hash} has at least one reference left")
        return

    # Unpin the file from IPFS or remove it from local storage
    storage_detected: ItemType = item_type_from_hash(storage_hash)

    if storage_type != storage_detected:
        raise ValueError(f"Inconsistent ItemType {storage_type} != {storage_detected} "
                         f"for hash '{storage_hash}'")

    if storage_type == ItemType.ipfs:
        api = await get_ipfs_api(timeout=5)
        logger.debug(f"Removing from IPFS: {storage_hash}")
        try:
            result = await api.pin.rm(storage_hash)
            print(result)

            # Launch the IPFS garbage collector (`ipfs repo gc`)
            async for _ in RepoAPI(driver=api).gc():
                pass

        except NotPinnedError:
            logger.debug("File not pinned")
        logger.debug(f"Removed from IPFS: {storage_hash}")
    elif storage_type == ItemType.storage:
        logger.debug(f"Removing from Gridfs: {storage_hash}")
        await delete_value(storage_hash)
        logger.debug(f"Removed from Gridfs: {storage_hash}")
    else:
        raise ValueError(f"Invalid storage type {storage_type}")
    logger.debug(f"Removed from {storage_type}: {storage_hash}")


async def is_allowed_to_forget(target: Dict, by: ForgetMessage) -> bool:
    """Check if a forget message is allowed to 'forget' the target message given its hash.
    """
    # Both senders are identical:
    if by.sender == target.get("sender"):
        return True
    else:
        # The forget sender matches the content address:
        target_content = await get_message_content(target)
        if by.sender == target_content.value["address"]:
            return True
    return False


async def forget_if_allowed(target_message: Dict, forget_message: ForgetMessage) -> None:
    """Forget a message.

    Remove the ‘content’ and ‘item_content’ sections of the targeted messages.
    Add a field ‘removed_by’ that references to the processed FORGET message.
    """
    target_hash = target_message["item_hash"]

    if not target_message:
        logger.info(f"Message to forget could not be found with id {target_hash}")
        return

    if target_message.get("type") == MessageType.forget:
        logger.info(f"FORGET message may not be forgotten {target_hash} by {forget_message.item_hash}")
        return

    if not await is_allowed_to_forget(target_message, by=forget_message):
        logger.info(f"Not allowed to forget {target_hash} by {forget_message.item_hash}")
        return

    if target_message.get("content") is None:
        logger.debug(f"Message content already forgotten: {target_message}")
        return

    logger.debug(f"Removing content for {target_hash}")
    updates = {
        "content": None,
        "item_content": None,
        "forgotten_by": [forget_message.item_hash],
    }
    await Message.collection.update_many(filter={"item_hash": target_hash}, update={"$set": updates})

    # TODO QUESTION: Should the removal be added to the CappedMessage collection for websocket
    #  updates ? Forget messages should already be published there, but the logic to validate
    #  them could be centralized here.

    if target_message.get("type") == MessageType.store:
        store_content = StoreContent(**target_message["content"])
        await garbage_collect(store_content.item_hash, store_content.item_type)


async def handle_forget_message(message: Dict, content: Dict):
    # Parsing and validation
    forget_message = ForgetMessage(**message, content=content)
    logger.debug(f"Handling forget message {forget_message.item_hash}")

    for target_hash in forget_message.content.hashes:
        target_message = await Message.collection.find_one(filter={"item_hash": target_hash})
        if target_message is None:
            logger.info(f"Message to forget could not be found with id {target_hash}")
            continue

        await forget_if_allowed(target_message=target_message, forget_message=forget_message)

    return True
