import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import grpc
import grpc.aio
import multiaddr as multiaddr_lib
from configmanager import Config

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.services.p2p.client import P2PGrpcClient
from aleph.services.peers.allowlist import CORECHANNEL_KEY, extract_peer_id
from aleph.types.db_session import DbSessionFactory

from ..cache.node_cache import NodeCache
from .http import api_get_request


@dataclass
class PeerStatus:
    peer_uri: str
    is_online: bool
    version: Optional[str]


LOGGER = logging.getLogger("P2P.jobs")


async def check_peer(peer_uri: str, timeout: int = 1) -> PeerStatus:
    try:
        version_info = await api_get_request(peer_uri, "version", timeout=timeout)
        if version_info is not None:
            return PeerStatus(peer_uri=peer_uri, is_online=True, version=version_info)

    except Exception:
        LOGGER.exception("Can't contact peer %r" % peer_uri)

    return PeerStatus(peer_uri=peer_uri, is_online=False, version=None)


def api_servers_from_aggregate(content: Dict[str, Any]) -> List[str]:
    """
    Extracts the HTTP API endpoints of active CCNs from the corechannel
    aggregate. Replaces the ALIVE-fed peers table as the source of the
    legacy HTTP fallback server list: stake-authenticated, and available
    as soon as the node has synced the aggregate.
    """
    servers: List[str] = []
    for node in content.get("nodes", []):
        if node.get("status") != "active":
            continue
        address = (node.get("address") or "").strip().rstrip("/")
        if not address.startswith(("http://", "https://")):
            continue
        if address not in servers:
            servers.append(address)
    return servers


async def tidy_http_peers_job(
    config: Config, session_factory: DbSessionFactory, node_cache: NodeCache
) -> None:
    """Keeps node_cache.api_servers in sync with reachable active CCN APIs."""
    from aleph.services.utils import get_IP

    corechannel_address = config.aleph.corechannel.address.value
    my_ip = await get_IP()
    await asyncio.sleep(2)

    while True:
        jobs = []

        try:
            with session_factory() as session:
                aggregate = get_aggregate_by_key(
                    session=session,
                    owner=corechannel_address,
                    key=CORECHANNEL_KEY,
                )

            candidates = (
                api_servers_from_aggregate(aggregate.content)
                if aggregate is not None and aggregate.content is not None
                else []
            )

            for server in candidates:
                if my_ip in server:
                    continue
                jobs.append(check_peer(server))
            peer_statuses = await asyncio.gather(*jobs)

            # Add reachable registry servers, drop anything cached that is
            # now unreachable or no longer in the registry.
            reachable = {
                status.peer_uri for status in peer_statuses if status.is_online
            }
            cached = await node_cache.get_api_servers()
            for server in reachable - cached:
                await node_cache.add_api_server(server)
            for server in cached - reachable:
                await node_cache.remove_api_server(server)

        except Exception:
            LOGGER.exception("Error refreshing the API server list")

        await asyncio.sleep(config.p2p.reconnect_delay.value)


def preferred_peers_from_aggregate(
    content: Dict[str, Any]
) -> List[Tuple[str, List[str]]]:
    """
    Extracts (peer_id, [multiaddr, ...]) pairs for CCNs from the corechannel
    aggregate content.

    Only nodes with status "active" are included (the allowlist intentionally
    does NOT filter by status; preferred slots are a capped resource so we
    apply the stricter filter here).

    Nodes whose multiaddress fails strict multiaddr parsing are skipped;
    nodes without a parsable /p2p/ component are also skipped. Several
    records with the same peer ID are merged.
    """
    peers: Dict[str, List[str]] = {}
    for node in content.get("nodes", []):
        if node.get("status") != "active":
            continue
        multiaddress = node.get("multiaddress") or ""
        try:
            multiaddr_lib.Multiaddr(multiaddress)
        except ValueError:
            LOGGER.debug("Skipping node with invalid multiaddress: %r", multiaddress)
            continue
        peer_id = extract_peer_id(multiaddress)
        if not peer_id:
            continue
        addrs = peers.setdefault(peer_id, [])
        if multiaddress not in addrs:
            addrs.append(multiaddress)
    return list(peers.items())


async def refresh_preferred_peers_job(
    config: Config,
    session_factory: DbSessionFactory,
    p2p_client: P2PGrpcClient,
) -> None:
    """
    Periodically pushes the registered CCN peer set (from the corechannel
    aggregate) to the P2P service. Preferred peers get protected connection
    slots and a gossipsub score bonus. This only ever upgrades peers: an
    empty or missing aggregate simply means no preferences are pushed.
    """
    corechannel_address = config.aleph.corechannel.address.value
    interval = config.aleph.corechannel.cache_ttl.value

    while True:
        try:
            with session_factory() as session:
                aggregate = get_aggregate_by_key(
                    session=session,
                    owner=corechannel_address,
                    key=CORECHANNEL_KEY,
                )

            if aggregate is not None and aggregate.content is not None:
                peers = preferred_peers_from_aggregate(aggregate.content)
                # Drop our own entry: no value in treating self as a preferred peer.
                peers = [
                    (peer_id, addrs)
                    for peer_id, addrs in peers
                    if peer_id != p2p_client.peer_id
                ]
                if peers:
                    accepted, truncated = await p2p_client.set_preferred_peers(peers)
                    LOGGER.info(
                        "Pushed %d preferred peers to the P2P service"
                        " (%d accepted, %d truncated)",
                        len(peers),
                        accepted,
                        truncated,
                    )
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                LOGGER.debug("P2P service does not support preferred peers yet")
            else:
                LOGGER.exception("Error refreshing preferred peers")
        except Exception:
            LOGGER.exception("Error refreshing preferred peers")

        await asyncio.sleep(interval)
