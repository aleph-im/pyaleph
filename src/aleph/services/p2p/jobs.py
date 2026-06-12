import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import grpc
import grpc.aio
import multiaddr as multiaddr_lib
from configmanager import Config

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.db.accessors.peers import get_all_addresses_by_peer_type
from aleph.db.models import PeerType
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


async def tidy_http_peers_job(
    config: Config, session_factory: DbSessionFactory, node_cache: NodeCache
) -> None:
    """Check that HTTP peers are reachable, else remove them from the list"""
    from aleph.services.utils import get_IP

    my_ip = await get_IP()
    await asyncio.sleep(2)

    while True:
        jobs = []

        try:
            with session_factory() as session:
                peers = get_all_addresses_by_peer_type(
                    session=session, peer_type=PeerType.HTTP
                )

            for peer in peers:
                if my_ip in peer:
                    continue

                jobs.append(check_peer(peer))
            peer_statuses = await asyncio.gather(*jobs)

            for peer_status in peer_statuses:
                peer_in_api_servers = await node_cache.has_api_server(
                    peer_status.peer_uri
                )

                if peer_status.is_online:
                    if not peer_in_api_servers:
                        await node_cache.add_api_server(peer_status.peer_uri)

                else:
                    if peer_in_api_servers:
                        await node_cache.remove_api_server(peer_status.peer_uri)

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

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
