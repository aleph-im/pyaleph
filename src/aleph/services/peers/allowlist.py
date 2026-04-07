import logging
import time
from typing import Optional, Set

from configmanager import Config

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)

CORECHANNEL_KEY = "corechannel"

DEFAULT_CACHE_TTL = 300  # 5 minutes


def _extract_peer_id(multiaddress: str) -> Optional[str]:
    """Extract the peer ID from a multiaddress ending with /p2p/<peer_id>."""
    parts = multiaddress.split("/p2p/")
    if len(parts) == 2 and parts[1]:
        return parts[1]
    return None


class PeerAllowlist:
    """
    Controls which peers are allowed to register via alive messages.

    Always allows bootstrap peers from config. Once the corechannel aggregate
    is available locally, also allows all CCN peer IDs found in it.
    """

    def __init__(
        self,
        session_factory: DbSessionFactory,
        bootstrap_peer_ids: Set[str],
        corechannel_address: str,
        cache_ttl: int = DEFAULT_CACHE_TTL,
    ):
        self._session_factory = session_factory
        self._bootstrap_peer_ids = bootstrap_peer_ids
        self._corechannel_address = corechannel_address
        self._cache_ttl = cache_ttl
        self._cached_ccn_peer_ids: Set[str] = set()
        self._cache_timestamp: float = 0

    @classmethod
    def from_config(
        cls, config: Config, session_factory: DbSessionFactory
    ) -> "PeerAllowlist":
        bootstrap_peer_ids: Set[str] = set()
        for peer_maddr in config.p2p.peers.value:
            peer_id = _extract_peer_id(peer_maddr)
            if peer_id:
                bootstrap_peer_ids.add(peer_id)

        LOGGER.info(
            "Peer allowlist initialized with %d bootstrap peers",
            len(bootstrap_peer_ids),
        )
        return cls(
            session_factory=session_factory,
            bootstrap_peer_ids=bootstrap_peer_ids,
            corechannel_address=config.aleph.corechannel.address.value,
            cache_ttl=config.aleph.corechannel.cache_ttl.value,
        )

    def _refresh_ccn_peer_ids(self) -> Set[str]:
        try:
            with self._session_factory() as session:
                aggregate = get_aggregate_by_key(
                    session=session,
                    owner=self._corechannel_address,
                    key=CORECHANNEL_KEY,
                )

            if aggregate is None or aggregate.content is None:
                return set()

            peer_ids: Set[str] = set()
            for node in aggregate.content.get("nodes", []):
                multiaddress = node.get("multiaddress", "")
                if multiaddress:
                    peer_id = _extract_peer_id(multiaddress)
                    if peer_id:
                        peer_ids.add(peer_id)

            LOGGER.info(
                "Loaded %d CCN peer IDs from corechannel aggregate", len(peer_ids)
            )
            return peer_ids

        except Exception:
            LOGGER.exception("Failed to load corechannel aggregate")
            return self._cached_ccn_peer_ids

    def _ensure_cache_fresh(self):
        now = time.monotonic()
        if now - self._cache_timestamp > self._cache_ttl:
            self._cached_ccn_peer_ids = self._refresh_ccn_peer_ids()
            self._cache_timestamp = now

    def is_allowed(self, peer_id: str) -> bool:
        if peer_id in self._bootstrap_peer_ids:
            return True

        self._ensure_cache_fresh()
        return peer_id in self._cached_ccn_peer_ids
