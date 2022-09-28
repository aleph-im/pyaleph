""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- handle garbage collection of unused hashes
"""

import asyncio
import logging
from typing import Optional

import aioipfs
from aioipfs import InvalidCIDError
from aleph_message.models import ItemType

from aleph.config import get_config
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.schemas.validated_message import (
    StoreContentWithMetadata,
    ValidatedStoreMessage,
    EngineInfo,
)
from aleph.services.ipfs.common import get_ipfs_api
from aleph.storage import StorageService
from aleph.utils import item_type_from_hash

LOGGER = logging.getLogger("HANDLERS.STORAGE")


class StoreMessageHandler:
    def __init__(self, storage_service: StorageService):
        self.storage_service = storage_service

    async def handle_new_storage(
        self, store_message: ValidatedStoreMessage
    ) -> Optional[bool]:
        config = get_config()
        if not config.storage.store_files.value:
            return True  # Ignore

        item_type = store_message.content.item_type
        try:
            engine = ItemType(item_type)
        except ValueError:
            LOGGER.warning("Got invalid storage engine %s" % item_type)
            return False  # not allowed, ignore.

        output_content = StoreContentWithMetadata.from_content(store_message.content)

        is_folder = False
        item_hash = store_message.content.item_hash

        ipfs_enabled = config.ipfs.enabled.value
        do_standard_lookup = True

        if engine == ItemType.ipfs and ipfs_enabled:
            if item_type_from_hash(item_hash) != ItemType.ipfs:
                LOGGER.warning("Invalid IPFS hash: '%s'", item_hash)
                raise UnknownHashError(f"Invalid IPFS hash: '{item_hash}'")

            api = await get_ipfs_api(timeout=5)
            try:
                try:
                    stats = await asyncio.wait_for(
                        api.files.stat(f"/ipfs/{item_hash}"), 5
                    )
                except InvalidCIDError as e:
                    raise UnknownHashError(
                        f"Invalid IPFS hash from API: '{item_hash}'"
                    ) from e
                if stats is None:
                    return None

                if (
                    stats["Type"] == "file"
                    and stats["CumulativeSize"] < 1024**2
                    and len(item_hash) == 46
                ):
                    do_standard_lookup = True
                else:
                    output_content.size = stats["CumulativeSize"]
                    output_content.engine_info = EngineInfo(**stats)
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
                file_content = await self.storage_service.get_hash_content(
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

            output_content.size = len(file_content)

        output_content.content_type = "directory" if is_folder else "file"
        store_message.content = output_content

        return True
