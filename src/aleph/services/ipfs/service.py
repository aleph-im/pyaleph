import asyncio
import concurrent
import json
import logging
from typing import AsyncIterable, Dict, Optional, Self, Union, cast

import aiohttp
import aioipfs
from configmanager import Config

from aleph.services.ipfs.common import make_ipfs_p2p_client, make_ipfs_pinning_client
from aleph.services.utils import get_IP
from aleph.types.message_status import FileUnavailable
from aleph.utils import run_in_executor

LOGGER = logging.getLogger(__name__)

MAX_LEN = 1024 * 1024 * 100


async def fetch_raw_cid_streamed(
    aioipfs_client: aioipfs.AsyncIPFS,
    chunk_size: int,
    params: Optional[Dict] = None,
) -> AsyncIterable[bytes]:
    driver = aioipfs_client.core.driver
    params = params or {}

    async with driver.session.post(
        aioipfs_client.core.url("cat"), params=params, auth=driver.auth
    ) as response:
        if response.status in aioipfs.apis.HTTP_ERROR_CODES:
            data = await response.read()
            aioipfs_client.core.handle_error(response, data)

        async for chunk in response.content.iter_chunked(chunk_size):
            yield chunk


class IpfsService:
    def __init__(
        self,
        ipfs_client: aioipfs.AsyncIPFS,
        pinning_client: Optional[aioipfs.AsyncIPFS] = None,
    ):
        self.ipfs_client = ipfs_client  # For P2P operations
        self.pinning_client = pinning_client or ipfs_client  # For pinning operations

    @classmethod
    def new(cls, config: Config) -> Self:
        # Create P2P client (for content retrieval, pubsub)
        p2p_client = make_ipfs_p2p_client(config)

        # Create separate pinning client if configured differently
        if _should_use_separate_pinning_client(config):
            LOGGER.info("Using separate IPFS client for pinning operations")
            pinning_client = make_ipfs_pinning_client(config)
        else:
            pinning_client = p2p_client

        return cls(ipfs_client=p2p_client, pinning_client=pinning_client)

    async def __aenter__(self):
        return self

    async def close(self):
        await self.ipfs_client.close()
        # Only close pinning client if it's different from main client
        if self.pinning_client != self.ipfs_client:
            await self.pinning_client.close()

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

    async def get_ipfs_size(
        self, hash: str, timeout: int = 1, tries: int = 1
    ) -> Optional[int]:
        try_count = 0
        result = None
        while (result is None) and (try_count < tries):
            try_count += 1
            try:
                dag_node = await asyncio.wait_for(
                    self.ipfs_client.dag.get(hash), timeout=timeout
                )
                result = 0
                if isinstance(dag_node, str):
                    dag_node = json.loads(dag_node)
                if isinstance(dag_node, dict):
                    if "Data" in dag_node and isinstance(dag_node["Data"], dict):
                        # This is the common structure for UnixFS nodes after aioipfs parsing
                        if "filesize" in dag_node["Data"]:
                            result = dag_node["Data"]["filesize"]
                        elif (
                            "Tsize" in dag_node["Data"]
                        ):  # Less common, but good to check
                            result = dag_node["Data"]["Tsize"]
                        elif (
                            "Tsize" in dag_node
                        ):  # Sometimes it might be at the top level directly
                            result = dag_node["Tsize"]
                    if (
                        result == 0
                        and "Links" in dag_node
                        and isinstance(dag_node["Links"], list)
                    ):
                        total_size = 0
                        for link in dag_node["Links"]:
                            # In case it's a link list, get the Tsize property if exists
                            if "Tsize" in link and isinstance(link["Tsize"], int):
                                total_size += link["Tsize"]
                            else:
                                LOGGER.error(
                                    f"Error: CID {hash} did not return a list structure. Type: {type(link)}"
                                )
                        result = total_size

                    elif (
                        "Size" in dag_node
                    ):  # Occasionally, 'Size' might refer to total size, but often it's block
                        # size
                        result = dag_node["Size"]
                else:
                    # For raw blocks, dag_node might be bytes. block.stat is better for those.
                    # For other codecs, the structure will vary.
                    LOGGER.info(
                        f"Warning: CID {hash} did not return a dictionary structure. Type: {type(dag_node)}"
                    )

                if result == 0:
                    LOGGER.info(
                        f"INFO: CID {hash} didn't return a Size field. Executing a block stat operation"
                    )
                    block_stat = await asyncio.wait_for(
                        self.ipfs_client.block.stat(hash), timeout=timeout
                    )
                    result = block_stat["Size"]
            except aioipfs.APIError:
                result = None
                await asyncio.sleep(0.5)
                continue
            except asyncio.TimeoutError:
                raise FileUnavailable("Could not retrieve IPFS content at this time")
            except (
                concurrent.futures.CancelledError,
                aiohttp.client_exceptions.ClientConnectorError,
            ):
                try_count -= 1  # do not count as a try.
                await asyncio.sleep(0.1)

        return result

    async def get_ipfs_content(
        self, hash: str, timeout: int = 1, tries: int = 1
    ) -> Optional[bytes]:
        try_count = 0
        result = None
        while (result is None) and (try_count < tries):
            try_count += 1
            try:
                result = cast(
                    bytes,
                    await asyncio.wait_for(
                        self.ipfs_client.cat(hash, length=MAX_LEN), timeout=timeout
                    ),
                )
                if len(result) == MAX_LEN:
                    result = None
                    break
            except aioipfs.APIError:
                result = None
                await asyncio.sleep(0.5)
                continue
            except asyncio.TimeoutError:
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

    async def get_ipfs_content_iterator(
        self, cid: str, chunk_size: int
    ) -> Optional[AsyncIterable[bytes]]:
        params = {aioipfs.helpers.ARG_PARAM: cid}
        return fetch_raw_cid_streamed(
            aioipfs_client=self.ipfs_client,
            chunk_size=chunk_size,
            params=params,
        )

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
        result = self.pinning_client.add_json(value)
        return result["Hash"]

    async def add_bytes(self, value: bytes, cid_version: int = 0) -> str:
        result = await self.pinning_client.add_bytes(value, cid_version=cid_version)
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

        # Use pinning client instead of main client
        async for status in self.pinning_client.pin.add(cid):
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
                    raise FileUnavailable(
                        f"Could not pin IPFS content at this time ({details})"
                    )
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

    async def sub(self, topic: str):
        ipfs_client = self.ipfs_client

        try:
            async for mvalue in ipfs_client.pubsub.sub(topic):
                try:
                    LOGGER.debug("New message received %r" % mvalue)

                    # we should check the sender here to avoid spam
                    # and such things...
                    yield mvalue

                except Exception:
                    LOGGER.exception("Error handling message")
        except Exception:
            LOGGER.exception("Error handling IPFS subscription")
            await self.close()

    async def pub(self, topic: str, message: Union[str, bytes]):
        # aioipfs only accepts strings
        message_str = message if isinstance(message, str) else message.decode("utf-8")

        ipfs_client = self.ipfs_client
        await ipfs_client.pubsub.pub(topic, message_str)


def _should_use_separate_pinning_client(config: Config) -> bool:
    """
    Determine if we should use a separate IPFS client for pinning operations.
    Returns True if pinning configuration is different from main IPFS configuration.
    """
    if not hasattr(config.ipfs, "pinning"):
        return False

    # Check if pinning host/port are specifically configured and different
    pinning_host = config.ipfs.pinning.host.value
    pinning_port = config.ipfs.pinning.port.value

    if pinning_host and pinning_port:
        main_host = config.ipfs.host.value
        main_port = config.ipfs.port.value
        return (pinning_host != main_host) or (pinning_port != main_port)

    return False
