""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- hjandle garbage collection of unused hashes
"""

import logging

import aioipfs
import asyncio

from aioipfs import UnknownAPIError

from aleph.services.ipfs.common import get_ipfs_api
from aleph.storage import get_hash_content
from aleph.types import ItemType, UnknownHashError
from aleph.web import app

LOGGER = logging.getLogger("HANDLERS.STORAGE")


async def handle_new_storage(message, content):
    if not app["config"].storage.store_files.value:
        return True  # Ignore

    item_type = content.get("item_type")
    try:
        engine = ItemType(item_type)
    except ValueError:
        LOGGER.warning("Got invalid storage engine %s" % item_type)
        return -1  # not allowed, ignore.

    is_folder = False
    item_hash = content["item_hash"]
    ipfs_enabled = app["config"].ipfs.enabled.value
    do_standard_lookup = True
    size = 0

    if engine == ItemType.IPFS and ipfs_enabled:
        if ItemType.from_hash(item_hash) != ItemType.IPFS:
            LOGGER.warning(f"Invalid IPFS hash: '{item_hash}'")
            raise UnknownHashError(f"Invalid IPFS hash: '{item_hash}'")

        api = await get_ipfs_api(timeout=5)
        try:
            try:
                stats = await asyncio.wait_for(api.files.stat(f"/ipfs/{item_hash}"), 5)
            except UnknownAPIError:
                raise UnknownHashError(f"Invalid IPFS hash from API: '{item_hash}'")
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
                    if timer > 30 and status["Pins"] is None:
                        return None  # Can't retrieve data now.
                do_standard_lookup = False

        except (aioipfs.APIError, asyncio.TimeoutError) as e:
            if hasattr(e, "message"):
                if "invalid CID" in getattr(e, "message", ""):
                    LOGGER.warning(
                        f"Error retrieving stats of hash {item_hash}: {e.message}"
                    )
                    return -1

                LOGGER.exception(
                    f"Error retrieving stats of hash {item_hash}: {e.message}"
                )
                do_standard_lookup = True
            else:
                LOGGER.exception(f"Error retrieving stats of hash {item_hash}")
                do_standard_lookup = True

    if do_standard_lookup:
        # TODO: We should check the balance here.
        file_content = await get_hash_content(
            item_hash,
            engine=engine,
            tries=4,
            timeout=2,
            use_network=True,
            use_ipfs=True,
            store_value=True,
        )
        if file_content is None or file_content == -1:
            return None

        size = len(file_content)

    content["size"] = size
    content["content_type"] = is_folder and "directory" or "file"

    return True
