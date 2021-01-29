import asyncio
import logging
import time
from dataclasses import dataclass
from functools import partial
from typing import Dict, Iterator, NewType, Optional, Coroutine

import aiohttp
import motor.motor_asyncio
# import libp2p
from dataclasses_json import dataclass_json
from libp2p import new_node, BasicHost
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.peer.peerinfo import info_from_p2p_addr
from multiaddr import multiaddr, Multiaddr

logger = logging.getLogger()
Url = NewType('Url', str)


@dataclass_json
@dataclass
class Node:
    """Node data from the aggregate."""
    hash: str
    owner: str
    reward: str
    stakers: dict
    total_staked: float
    status: str
    time: float
    name: str
    multiaddress: str
    picture: str
    banner: str
    description: str


@dataclass
class NodeMetrics:
    """Metrics obtained from a node."""
    error: Optional[Exception] = None
    p2p_connect_latency: Optional[float] = None
    http_index_latency: Optional[float] = None
    http_aggregate_latency: Optional[float] = None
    http_store_latency: Optional[float] = None

    def global_score(self):
        if self.error is None and self.p2p_connect_latency:
            return max(1-self.p2p_connect_latency or 1, 0) * \
                   max(1-self.http_index_latency or 1, 0) * \
                   max(1-self.http_aggregate_latency or 1, 0) * \
                   max(1-(self.http_store_latency or 1), 0)
        else:
            return 0

    def __str__(self):
        if self.error:
            return str(self.error)
        else:
            return f"{self.p2p_connect_latency} {self.http_index_latency} {self.http_aggregate_latency} {self.http_store_latency} -> {self.global_score()}"


async def get_aggregate(url: Url):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            print(resp.status)
            return await resp.json()


def nodes_from_aggregate(aggregate: Dict) -> Iterator[Node]:
    for node in aggregate['data']['corechannel']['nodes']:
        yield Node.from_dict(node)


async def measure_coroutine_latency(coroutine: Coroutine, timeout: float=2.) -> Optional[float]:
    """Execute a coroutine and return how much time it took to execute."""
    t0 = time.time()
    dt: float
    try:
        await asyncio.wait_for(coroutine, timeout=timeout)
        return time.time() - t0
    except asyncio.TimeoutError:
        return None


async def p2p_connect(host: BasicHost, address: Multiaddr) -> Optional[float]:
    """Connect to a node using libp2p."""
    info = info_from_p2p_addr(address)
    await host.connect(info),


async def http_get_index(address: Multiaddr) -> aiohttp.ClientResponse:
    """Get the index of a node using the HTTP API"""
    ip4 = address.value_for_protocol('ip4')
    url = f"http://{ip4}:4024/"

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                return resp
            else:
                resp.raise_for_status()


async def http_get_aggregate(address: Multiaddr) -> aiohttp.ClientResponse:
    """Get a copy of the aggregate from a node using the HTTP API"""
    ip4 = address.value_for_protocol('ip4')
    url = f"http://{ip4}:4024/api/v0/aggregates/0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10.json"

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                return resp
            else:
                resp.raise_for_status()


store_message = {
        "_id" : "6009dbdd9b6d38b89d36bb4a",
        "chain" : "ETH",
        "item_hash" : "ddb72fa6751d59546ad3156bb250151d9308be6c957e9ac57d297ec566ae806a",
        "sender" : "0x6696450BA99d0bDcAB713c33F2bf106D746485D2",
        "type" : "STORE",
        "channel" : "MYALEPH",
        "confirmations" : [
                {
                        "chain" : "ETH",
                        "height" : 11528852,
                        "hash" : "0x88870e4d4c16bc36ba356421d7926917b80d0d5d8a06471e6463ea7895161a24"
                }
        ],
        "content" : {
                "address" : "0x6696450BA99d0bDcAB713c33F2bf106D746485D2",
                "item_type" : "ipfs",
                "item_hash" : "QmaQVXbihn3Ln2oSrVdL1oby832pxthXN9j669E9hwugVd",
                "time" : 1608978939.585,
                "engine_info" : {
                        "Hash" : "QmaQVXbihn3Ln2oSrVdL1oby832pxthXN9j669E9hwugVd",
                        "Size" : 72639,
                        "CumulativeSize" : 72653,
                        "Blocks" : 0,
                        "Type" : "file"
                },
                "size" : 72653,
                "content_type" : "file"
        },
        "item_content" : "{\"address\":\"0x6696450BA99d0bDcAB713c33F2bf106D746485D2\",\"item_type\":\"ipfs\",\"item_hash\":\"QmaQVXbihn3Ln2oSrVdL1oby832pxthXN9j669E9hwugVd\",\"time\":1608978939.585}",
        "item_type" : "inline",
        "signature" : "0xa80a8f30d03a2d87f1d1f6be568627ad7009314238305fbfe853e74620e90222738ab81dcc86a8e0d1fdbf65c927d7892a4d10d4513a810edfb1093348d46a5c1b",
        "size" : 158,
        "time" : 1608979428.008
}


def get_ipfs_cid_from_url(url: str) -> bytes:
    """Compute the IPFS CID of the data downloaded at an url.

    Streams the data to IPFS, hoping to avoid storing the entire file in memory or disk.
    $ curl --output - "https://releases.ubuntu.com/20.04.1/ubuntu-20.04.1-desktop-amd64.iso" | ipfs add --only-hash -Q
    """
    from subprocess import Popen, PIPE
    curl_process = Popen(['curl', '--output', '-', '--no-progress-meter', url], stdout=PIPE)
    ipfs_process = Popen(['ipfs', 'add', '--only-hash', '-Q'],
                         stdin=curl_process.stdout, stdout=PIPE)
    curl_process.stdout.close()  # enable write error in dd if ssh dies
    out, err = ipfs_process.communicate()

    logger.debug("Downloading URL", url)
    if err:
        raise ValueError(err)
    return out.strip()


async def http_get_stored_file(address: Multiaddr) -> bool:
    """Get a file from a node using the HTTP API"""
    ip4 = address.value_for_protocol('ip4')
    item_hash = store_message['content']['item_hash'].encode()  #TODO: Pick random message
    url = f"http://{ip4}:4024/api/v0/storage/raw/{item_hash.decode()}"

    loop = asyncio.get_event_loop()
    served_hash = await loop.run_in_executor(None, partial(get_ipfs_cid_from_url, url))

    if item_hash != served_hash:
        raise ValueError(f"Hashes differ: '{item_hash.decode()}' != '{served_hash.decode()}'")


async def get_message_store():
    client = motor.motor_asyncio.AsyncIOMotorClient()
    db = client.alephtest
    message = db.messages.find({}).limit(1).skip(r)


secret = b'#\xb8\xc1\xe99$V\xde>\xb1;\x90FhRW\xbd\xd6@\xfb\x06g\x1a\xd1\x1c\x801\x7f\xa3\xb1y\x9d'
transport_opt = f"/ip4/127.0.0.1/tcp/1234"


async def main():
    aggregate_url = "https://api2.aleph.im/api/v0/aggregates/0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10.json"
    aggregate = await get_aggregate(aggregate_url)

    p2p_host = await new_node(
        key_pair=create_new_key_pair(secret), transport_opt=[transport_opt]
    )

    nodes = list(nodes_from_aggregate(aggregate))

    scores: Dict[str, NodeMetrics] = {}
    for node in nodes:
        metrics = NodeMetrics()
        try:
            address = Multiaddr(node.multiaddress)

            print('=>', address.protocols())
            metrics.p2p_connect_latency = await measure_coroutine_latency(p2p_connect(p2p_host, address))
            metrics.http_index_latency = await measure_coroutine_latency(http_get_index(address))
            metrics.http_aggregate_latency = await measure_coroutine_latency(http_get_aggregate(address))
            metrics.http_store_latency = await measure_coroutine_latency(http_get_stored_file(address))

        except multiaddr.exceptions.StringParseError as error:
            metrics.error = error
            logger.error(f"Invalid multiaddress {node.multiaddress}")
        except aiohttp.client_exceptions.ClientResponseError as error:
            metrics.error = error
            logger.error(f"Error {node.multiaddress}")
        except ValueError as error:
            metrics.error = error
            logger.error(f"Error {node.multiaddress}")
        finally:
            scores[node.hash] = metrics
            print(node.hash[:12], metrics)

    # Get a random message in MongoDB
    # db.collection.find(query).limit(1).skip(R))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

