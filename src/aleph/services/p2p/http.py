""" While our own streamer libp2p protocol is still unstable, use direct
HTTP connection to standard rest API.
"""

import asyncio
import base64
import logging
from random import sample
from typing import List, Optional, Sequence

import aiohttp

LOGGER = logging.getLogger("P2P.HTTP")


async def api_get_request(session: aiohttp.ClientSession, base_uri, method, timeout=1):
    uri = f"{base_uri}/api/v0/{method}"
    try:
        async with session.get(uri) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except (
        TimeoutError,
        asyncio.TimeoutError,
        ConnectionRefusedError,
        aiohttp.ClientError,
        OSError,
    ):
        result = None
    except Exception:
        LOGGER.exception("Error in retrieval")
        result = None
    return result


async def get_peer_hash_content(
    session: aiohttp.ClientSession,
    base_uri: str,
    item_hash: str,
    semaphore,
    timeout: int = 1,
) -> Optional[bytes]:
    async with (
        semaphore
    ):  # We use semaphore to avoid having too much call at the same time
        result = None
        item = await api_get_request(
            session=session,
            base_uri=base_uri,
            method=f"storage/{item_hash}",
            timeout=timeout,
        )
        if (
            item is not None
            and item["status"] == "success"
            and item["content"] is not None
        ):
            # TODO: IMPORTANT /!\ verify the hash of received data!
            return base64.decodebytes(item["content"].encode("utf-8"))
        else:
            LOGGER.debug(f"can't get hash {item_hash}")
        return result


async def request_hash(
    api_servers: Sequence[str], item_hash: str, timeout: int = 1
) -> Optional[bytes]:
    uris: List[str] = sample(api_servers, k=len(api_servers))

    # Avoid too  much request at the same time
    semaphore = asyncio.Semaphore(5)

    connector = aiohttp.TCPConnector(limit_per_host=5)
    timeout_conf = aiohttp.ClientTimeout(total=timeout)

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout_conf
    ) as session:
        # Use Task instead of
        tasks = [
            asyncio.create_task(
                get_peer_hash_content(
                    session=session,
                    base_uri=url,
                    item_hash=item_hash,
                    semaphore=semaphore,
                )
            )
            for url in uris
        ]

        for completed_task in asyncio.as_completed(tasks):
            try:
                result = await completed_task
                if result:
                    # We cancel other Task
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    return result
            except Exception:
                continue

    return None  # Nothing found...
