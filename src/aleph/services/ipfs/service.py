import asyncio
import concurrent
import json
import logging
from typing import IO, Optional, Union, Dict

import aiohttp
import aioipfs

from aleph.services.utils import get_IP
from aleph.utils import run_in_executor

LOGGER = logging.getLogger(__name__)

MAX_LEN = 1024 * 1024 * 100


class IpfsService:
    def __init__(self, ipfs_client: aioipfs.AsyncIPFS):
        self.ipfs_client = ipfs_client

    async def connect(self, peer: str) -> Dict:
        return await self.ipfs_client.swarm.connect(peer)

    async def get_public_address(self):
        public_ip = await get_IP()

        addresses = (await self.ipfs_client.id())["Addresses"]
        for address in addresses:
            if public_ip in address and "/tcp" in address and "/p2p" in address:
                return address

        # Fallback to first possible public...
        for address in addresses:
            if "127.0.0.1" not in address and "/tcp" in address and "/p2p" in address:
                return address

        # Still no public there, try ourselves.
        for address in addresses:
            if "127.0.0.1" in address and "/tcp" in address and "/p2p" in address:
                return address.replace("127.0.0.1", public_ip)

    async def get_ipfs_content(
        self, hash: str, timeout: int = 1, tries: int = 1
    ) -> Optional[bytes]:
        try_count = 0
        result = None
        while (result is None) and (try_count < tries):
            try_count += 1
            try:
                result = await asyncio.wait_for(
                    self.ipfs_client.cat(hash, length=MAX_LEN), timeout=timeout
                )
                if len(result) == MAX_LEN:
                    result = None
                    break
            except aioipfs.APIError:
                result = None
                await asyncio.sleep(0.5)
                continue
            except (asyncio.TimeoutError):
                result = None
                await asyncio.sleep(0.5)
            except (
                concurrent.futures.CancelledError,
                aiohttp.client_exceptions.ClientConnectorError,
            ):
                try_count -= 1  # do not count as a try.
                await asyncio.sleep(0.1)

        if isinstance(result, str):
            result = result.encode("utf-8")

        return result

    async def get_json(self, hash, timeout=1, tries=1):
        result = await self.get_ipfs_content(hash, timeout=timeout, tries=tries)
        if result is not None and result != -1:
            try:
                result = await run_in_executor(None, json.loads, result)
            except json.decoder.JSONDecodeError:
                # try:
                #     import json as njson
                #     result = await loop.run_in_executor(None, njson.loads, result)
                # except (json.JSONDecodeError, KeyError):
                LOGGER.exception("Can't decode JSON")
                result = -1  # never retry, bogus data
        return result

    async def add_json(self, value: bytes) -> str:
        result = self.ipfs_client.add_json(value)
        return result["Hash"]

    async def add_bytes(self, value: bytes, cid_version: int = 0) -> str:
        result = await self.ipfs_client.add_bytes(value, cid_version=cid_version)
        return result["Hash"]

    async def pin_add(self, hash: str, timeout: int = 2, tries: int = 1):
        try_count = 0
        result = None
        while (result is None) and (try_count < tries):
            try_count += 1
            try:
                result = None
                async for ret in self.ipfs_client.pin.add(hash):
                    result = ret
            except (asyncio.TimeoutError, json.JSONDecodeError):
                result = None
            except concurrent.futures.CancelledError:
                try_count -= 1  # do not count as a try.
                await asyncio.sleep(0.1)

        return result

    async def add_file(self, fileobject: IO):
        url = f"{self.ipfs_client.api_url}/api/v0/add"

        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field("path", fileobject)

            resp = await session.post(url, data=data)
            return await resp.json()

    async def sub(self, topic: str):
        ipfs_client = self.ipfs_client

        async for mvalue in ipfs_client.pubsub.sub(topic):
            try:
                LOGGER.debug("New message received %r" % mvalue)

                # we should check the sender here to avoid spam
                # and such things...
                yield mvalue

            except Exception:
                LOGGER.exception("Error handling message")

    async def pub(self, topic: str, message: Union[str, bytes]):
        ipfs_client = self.ipfs_client
        await ipfs_client.pubsub.pub(topic, message)
