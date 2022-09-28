import asyncio
import json
from dataclasses import asdict
from typing import Dict, Optional, List

from aleph.chains.common import LOGGER
from aleph.chains.tx_context import TxContext
from aleph.config import get_config
from aleph.exceptions import (
    InvalidContent,
    AlephStorageException,
    ContentCurrentlyUnavailable,
)
from aleph.model.filepin import PermanentPin
from aleph.model.pending import PendingTX

from aleph.storage import StorageService


class ChainDataService:
    def __init__(self, storage_service: StorageService):
        self.storage_service = storage_service

    async def get_chaindata(self, messages, bulk_threshold: int = 2000):
        """Returns content ready to be broadcasted on-chain (aka chaindata).

        If message length is over bulk_threshold (default 2000 chars), store list
        in IPFS and store the object hash instead of raw list.
        """
        chaindata = {
            "protocol": "aleph",
            "version": 1,
            "content": {"messages": messages},
        }
        content = json.dumps(chaindata)
        if len(content) > bulk_threshold:
            ipfs_id = await self.storage_service.add_json(chaindata)
            return json.dumps(
                {"protocol": "aleph-offchain", "version": 1, "content": ipfs_id}
            )
        else:
            return content

    async def get_chaindata_messages(
        self, chaindata: Dict, context: TxContext, seen_ids: Optional[List[str]] = None
    ):
        config = get_config()

        protocol = chaindata.get("protocol", None)
        version = chaindata.get("version", None)
        if protocol == "aleph" and version == 1:
            messages = chaindata["content"]["messages"]
            if not isinstance(messages, list):
                error_msg = f"Got bad data in tx {context!r}"
                raise InvalidContent(error_msg)
            return messages

        if protocol == "aleph-offchain" and version == 1:
            assert isinstance(chaindata.get("content"), str)
            if seen_ids is not None:
                if chaindata["content"] in seen_ids:
                    # is it really what we want here?
                    LOGGER.debug("Already seen")
                    return None
                else:
                    LOGGER.debug("Adding to seen_ids")
                    seen_ids.append(chaindata["content"])
            try:
                content = await self.storage_service.get_json(
                    chaindata["content"], timeout=10
                )
            except AlephStorageException:
                # Let the caller handle unavailable/invalid content
                raise
            except Exception as e:
                error_msg = (
                    f"Can't get content of offchain object {chaindata['content']!r}"
                )
                LOGGER.exception("%s", error_msg)
                raise ContentCurrentlyUnavailable(error_msg) from e

            try:
                messages = await self.get_chaindata_messages(content.value, context)
            except AlephStorageException:
                LOGGER.debug("Got no message")
                raise

            LOGGER.info("Got bulk data with %d items" % len(messages))
            if config.ipfs.enabled.value:
                try:
                    LOGGER.info(f"chaindata {chaindata}")
                    await PermanentPin.register(
                        multihash=chaindata["content"],
                        reason={
                            "source": "chaindata",
                            "protocol": chaindata["protocol"],
                            "version": chaindata["version"],
                        },
                    )
                    # Some IPFS fetches can take a while, hence the large timeout.
                    await asyncio.wait_for(
                        self.storage_service.pin_hash(chaindata["content"]), timeout=120
                    )
                except asyncio.TimeoutError:
                    LOGGER.warning(f"Can't pin hash {chaindata['content']}")
            return messages
        else:
            error_msg = f"Got unknown protocol/version object in tx {context!r}"
            LOGGER.info("%s", error_msg)
            raise InvalidContent(error_msg)

    @staticmethod
    async def incoming_chaindata(content: Dict, context: TxContext):
        """Incoming data from a chain.
        Content can be inline of "offchain" through an ipfs hash.
        For now we only add it to the database, it will be processed later.
        """
        await PendingTX.collection.insert_one(
            {"content": content, "context": asdict(context)}
        )
