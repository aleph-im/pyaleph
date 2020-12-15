import asyncio
import json
import logging
import platform
from dataclasses import dataclass, asdict
from typing import Dict, Optional
from urllib.parse import urljoin

import aiohttp
from aiocache import cached
from dataclasses_json import DataClassJsonMixin
from web3 import Web3

import aleph.model
from aleph import __version__
from aleph.web import app

LOGGER = logging.getLogger(__name__)


def format_dict_for_prometheus(values: Dict) -> str:
    """Format a dict to a Prometheus tags string"""
    values = (f"{key}={json.dumps(value)}"
              for key, value in values.items())
    return '{' + ','.join(values) + '}'


def format_dataclass_for_prometheus(instance) -> str:
    """Turn a dataclass into Prometheus text format"""

    result = []
    for key, value in asdict(instance).items():
        if isinstance(value, dict):
            # Use a constant value of 1 for version, as Prometheus does
            result.append(f"{key}{format_dict_for_prometheus(value)} 1")
        else:
            result.append(f"{key} {json.dumps(value)}")
    return '\n'.join(result)


@dataclass
class BuildInfo:
    """Dataclass used to export aleph node build info."""
    python_version: str
    version: str
    # branch: str
    # revision: str


@dataclass
class Metrics(DataClassJsonMixin):
    """Dataclass used to expose aleph node metrics.

    Naming convention: https://prometheus.io/docs/practices/naming/
    """
    pyaleph_build_info: BuildInfo
    pyaleph_status_sync_messages_total: int

    pyaleph_status_sync_pending_messages_total: int
    pyaleph_status_sync_pending_txs_total: int

    pyaleph_status_chain_eth_last_committed_height: int

    pyaleph_status_sync_messages_remaining_total: Optional[int] = None
    pyaleph_status_chain_eth_height_remaining_total: Optional[int] = None


pyaleph_build_info = BuildInfo(
    python_version=platform.python_version(),
    version=__version__,
)


# Cache Aleph messages count for 2 minutes
@cached(ttl=120)
async def fetch_reference_total_messages() -> Optional[int]:
    """Obtain the total number of Aleph messages from another node."""
    LOGGER.debug("Fetching Aleph messages count")

    url = app['config'].aleph.reference_node_url.value
    if url is None:
        return None

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(urljoin(url, 'metrics.json'), raise_for_status=True) as resp:
                data = await resp.json()
                return int(data['pyaleph_status_sync_messages_total'])
        except aiohttp.ClientResponseError:
            LOGGER.exception("ETH height could not be obtained")
            return None


# Cache ETH height for 10 minutes
@cached(ttl=600)
async def fetch_eth_height() -> Optional[int]:
    """Obtain the height of the Ethereum blockchain."""
    LOGGER.debug("Fetching ETH height")
    config = app['config']

    if config.ethereum.enabled.value:
        w3 = Web3(Web3.HTTPProvider(config.ethereum.api_url.value))
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, w3.eth.block_number)
    else:
        return None


async def get_metrics() -> Metrics:

    reference_total_messages = await fetch_reference_total_messages()
    reference_eth_height = await fetch_eth_height()

    sync_messages_total: int = await aleph.model.db.messages.estimated_document_count()

    eth_last_committed_height: int = (
            await aleph.model.db.chains.find_one({'name': 'ETH'},
                                                 projection={'last_commited_height': 1})
            or {}
    ).get('last_commited_height')

    if reference_total_messages:
        sync_messages_remaining_total = reference_total_messages - sync_messages_total
    else:
        sync_messages_remaining_total = None

    if reference_eth_height:
        eth_remaining_height = reference_eth_height - eth_last_committed_height
    else:
        eth_remaining_height = None

    return Metrics(
        pyaleph_build_info=pyaleph_build_info,

        pyaleph_status_sync_messages_total=sync_messages_total,

        pyaleph_status_sync_messages_remaining_total=sync_messages_remaining_total,

        pyaleph_status_sync_pending_messages_total=
        await aleph.model.db.pending_messages.estimated_document_count(),

        pyaleph_status_sync_pending_txs_total=
        await aleph.model.db.pending_txs.estimated_document_count(),

        pyaleph_status_chain_eth_last_committed_height=eth_last_committed_height,

        pyaleph_status_chain_eth_height_remaining_total=eth_remaining_height,
    )
