import asyncio
import concurrent
import json
import logging
from typing import IO, Optional, Union, Dict, Self

import aiohttp
import aioipfs
from configmanager import Config

from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.utils import get_IP
from aleph.types.message_status import FileUnavailable
from aleph.utils import run_in_executor

LOGGER = logging.getLogger(__name__)

MAX_LEN = 1024 * 1024 * 100


class IpfsService:
    def __init__(self, ipfs_client: aioipfs.AsyncIPFS):
        self.ipfs_client = ipfs_client

    @classmethod
    def new(cls, config: Config) -> Self:
        ipfs_client = make_ipfs_client(config)
        return cls(ipfs_client=ipfs_client)

    async def __aenter__(self):
        return self

    async def close(self):
        await self.ipfs_client.close()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

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

    async def _pin_add(self, cid: str, timeout: int = 30):
        # ipfs pin add returns a dictionary with a progress report in terms of blocks
        # and a "Pins" key if the pinning is complete. The dictionary is empty if the daemon
        # cannot find the file on the network.
        # A value is returned every half second by the daemon. We consider that the operation
        # fails if the same value is returned timeout * 2 times.
        # This assumes that the daemon progress never goes backwards.

        tick_timeout = timeout * 2
        last_progress = None

        async for status in self.ipfs_client.pin.add(cid):
            # If the Pins key appears, the file is pinned.
            if "Pins" in status:
                break

            progress = status.get("Progress")
            if progress == last_progress:
                tick_timeout -= 1
                if tick_timeout == 0:
                    if progress is None:
                        details = "file not found"
                    else:
                        details = "could not fetch some blocks"
                    raise FileUnavailable(f"Could not pin IPFS content at this time ({details})")
            else:
                # Reset the timeout counter if there is some measure of progress
                tick_timeout = timeout * 2

    async def pin_add(self, cid: str, timeout: int = 30, tries: int = 1):
        remaining_tries = tries

        while remaining_tries:
            try:
                await self._pin_add(cid=cid, timeout=timeout)
            except FileUnavailable:
                remaining_tries -= 1
                if not remaining_tries:
                    raise
            else:
                break

    async def add_file(self, file_content: bytes):
        url = f"{self.ipfs_client.api_url}add"

        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field("path", file_content)

            resp = await session.post(url, data=data)
            resp.raise_for_status()
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
        # aioipfs only accepts strings
        message_str = message if isinstance(message, str) else message.decode("utf-8")

        ipfs_client = self.ipfs_client
        await ipfs_client.pubsub.pub(topic, message_str)
