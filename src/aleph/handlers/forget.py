import logging

from aleph.model.messages import Message

logger = logging.getLogger(__name__)


async def count_file_references(storage_hash: str) -> int:
    """Count the number of references to a file on Aleph."""
    logger.debug(f"Counting documents for {storage_hash}")
    return await Message.collection.count_documents(
        filter={"content.item_hash": storage_hash},
    )
