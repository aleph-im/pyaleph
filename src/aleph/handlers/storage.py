""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- handle garbage collection of unused hashes
"""

import asyncio
import logging
from typing import Dict

import aioipfs
from aioipfs import InvalidCIDError
from aleph_message.models import ItemType, StoreMessage
from pydantic import ValidationError

from aleph.config import get_config
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.schemas.pending_messages import PendingStoreMessage
from aleph.services.ipfs.common import get_ipfs_api
from aleph.storage import get_hash_content
from aleph.utils import item_type_from_hash

LOGGER = logging.getLogger("HANDLERS.STORAGE")


async def handle_new_storage(message: PendingStoreMessage, content: Dict):
    config = get_config()
    if not config.storage.store_files.value:
        return True  # Ignore

    # TODO: ideally the content should be transformed earlier, but this requires more clean up
    #       (ex: no more in place modification of content, simplification of the flow)
    try:
        store_message = StoreMessage(**message.dict(exclude={"content"}), content=content)
    except ValidationError as e:
        print(e)
        return -1  # Invalid store message, discard

    item_type = store_message.content.item_type
    try:
        engine = ItemType(item_type)
    except ValueError:
        LOGGER.warning("Got invalid storage engine %s" % item_type)
        return -1  # not allowed, ignore.

    is_folder = False
    item_hash = store_message.content.item_hash

    ipfs_enabled = config.ipfs.enabled.value
    do_standard_lookup = True
    size = 0

    if engine == ItemType.ipfs and ipfs_enabled:
        if item_type_from_hash(item_hash) != ItemType.ipfs:
            LOGGER.warning("Invalid IPFS hash: '%s'", item_hash)
            raise UnknownHashError(f"Invalid IPFS hash: '{item_hash}'")

        api = await get_ipfs_api(timeout=5)
        try:
            try:
                stats = await asyncio.wait_for(api.files.stat(f"/ipfs/{item_hash}"), 5)
            except InvalidCIDError as e:
                raise UnknownHashError(f"Invalid IPFS hash from API: '{item_hash}'") from e
            if stats is None:
                return None

            if (
                stats["Type"] == "file"
                and stats["CumulativeSize"] < 1024 ** 2
                and len(item_hash) == 46
            ):
                do_standard_lookup = True
            else:
                size = stats["CumulativeSize"]
                content["engine_info"] = stats
                pin_api = await get_ipfs_api(timeout=60)
                timer = 0
                is_folder = stats["Type"] == "directory"
                async for status in pin_api.pin.add(item_hash):
                    timer += 1
                    if timer > 30 and "Pins" not in status:
                        return None  # Can't retrieve data now.
                do_standard_lookup = False

        except asyncio.TimeoutError as error:
            LOGGER.warning(
                f"Timeout while retrieving stats of hash {item_hash}: {getattr(error, 'message', None)}"
            )
            do_standard_lookup = True

        except aioipfs.APIError as error:
            LOGGER.exception(
                f"Error retrieving stats of hash {item_hash}: {getattr(error, 'message', None)}"
            )
            do_standard_lookup = True

    if do_standard_lookup:
        # TODO: We should check the balance here.
        try:
            file_content = await get_hash_content(
                item_hash,
                engine=engine,
                tries=4,
                timeout=2,
                use_network=True,
                use_ipfs=True,
                store_value=True,
            )
        except AlephStorageException:
            return None

        size = len(file_content)

    content["size"] = size
    content["content_type"] = is_folder and "directory" or "file"

    return True
