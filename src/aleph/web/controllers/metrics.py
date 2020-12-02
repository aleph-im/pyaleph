import json
import platform
from dataclasses import dataclass, asdict
from typing import Dict

import aleph.model
from aleph import __version__


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
class Metrics:
    """Dataclass used to expose aleph node metrics.
    """
    pyaleph_build_info: BuildInfo
    pyaleph_status_sync_messages_total: int
    pyaleph_status_sync_pending_messages_total: int
    pyaleph_status_chain_eth_last_committed_height: int


pyaleph_build_info = BuildInfo(
    python_version=platform.python_version(),
    version=__version__,
)


async def get_metrics() -> Metrics:

    return Metrics(
        pyaleph_build_info=pyaleph_build_info,

        pyaleph_status_sync_messages_total=
        await aleph.model.db.messages.count_documents({}),

        pyaleph_status_sync_pending_messages_total=
        await aleph.model.db.pending_messages.count_documents({}),

        pyaleph_status_chain_eth_last_committed_height=
        (await aleph.model.db.chains.find_one({'name': 'ETH'},
                                              projection={'last_commited_height': 1})
         or {}).get('last_commited_height'),
    )
