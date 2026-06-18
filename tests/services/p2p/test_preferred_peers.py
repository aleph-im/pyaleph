"""Tests for preferred-peers extraction and the refresh job."""

import asyncio
from contextlib import contextmanager
from typing import List, Optional, Sequence, Tuple
from unittest.mock import MagicMock

import pytest

from aleph.services.p2p.jobs import (
    preferred_peers_from_aggregate,
    refresh_preferred_peers_job,
)

# ---------------------------------------------------------------------------
# Real-looking peer IDs (valid base58 Qm... multihashes, 46 chars).
# The multiaddr library validates CIDs strictly, so we need proper values.
# ---------------------------------------------------------------------------
PEER_A = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
PEER_B = "QmYyQSo1c1Ym7orWxLYvCuxRjeKVxm7dXM4UNpgQeEHFKT"
PEER_C = "QmPChd2hVbrJ6bfo3WBcTW4iZnpHm8TEzWkLHmLpXhF68A"

MADDR_A = f"/ip4/51.1.2.3/tcp/4025/p2p/{PEER_A}"
MADDR_A2 = f"/ip4/51.1.2.4/tcp/4025/p2p/{PEER_A}"
MADDR_B = f"/dns/node.example.org/tcp/4025/p2p/{PEER_B}"
MADDR_C = f"/ip4/10.0.0.1/tcp/4025/p2p/{PEER_C}"


# ---------------------------------------------------------------------------
# Extraction unit tests
# ---------------------------------------------------------------------------


def test_extracts_peer_ids_and_multiaddrs():
    content = {
        "nodes": [
            {"multiaddress": MADDR_A, "status": "active"},
            {"multiaddress": MADDR_B, "status": "active"},
        ]
    }
    assert preferred_peers_from_aggregate(content) == [
        (PEER_A, [MADDR_A]),
        (PEER_B, [MADDR_B]),
    ]


def test_skips_nodes_without_usable_multiaddress():
    content = {
        "nodes": [
            {"multiaddress": "", "status": "active"},
            {"multiaddress": "/ip4/51.1.2.3/tcp/4025", "status": "active"},
            {"status": "active"},
            {"multiaddress": MADDR_C, "status": "active"},
        ]
    }
    assert preferred_peers_from_aggregate(content) == [(PEER_C, [MADDR_C])]


def test_deduplicates_peer_ids():
    content = {
        "nodes": [
            {"multiaddress": MADDR_A, "status": "active"},
            {"multiaddress": MADDR_A2, "status": "active"},
        ]
    }
    result = preferred_peers_from_aggregate(content)
    assert len(result) == 1
    assert result[0][0] == PEER_A
    assert result[0][1] == [MADDR_A, MADDR_A2]


def test_empty_content():
    assert preferred_peers_from_aggregate({}) == []
    assert preferred_peers_from_aggregate({"nodes": []}) == []


def test_skips_non_active_nodes():
    """Only nodes with status 'active' are included."""
    content = {
        "nodes": [
            {"multiaddress": MADDR_A, "status": "inactive"},
            {"multiaddress": MADDR_B, "status": "deactivated"},
            {"multiaddress": MADDR_C},  # no status key
            # Only this one should pass:
            {"multiaddress": MADDR_A, "status": "active"},
        ]
    }
    result = preferred_peers_from_aggregate(content)
    assert result == [(PEER_A, [MADDR_A])]


def test_skips_invalid_multiaddr():
    """Entries with a syntactically invalid multiaddress are skipped."""
    content = {
        "nodes": [
            {"multiaddress": "garbage-without-p2p-part", "status": "active"},
            {"multiaddress": "not_a_multiaddr", "status": "active"},
            {"multiaddress": MADDR_B, "status": "active"},
        ]
    }
    result = preferred_peers_from_aggregate(content)
    assert result == [(PEER_B, [MADDR_B])]


# ---------------------------------------------------------------------------
# Job-level tests
# ---------------------------------------------------------------------------


class FakeP2PClient:
    """Minimal fake with the same async interface as P2PGrpcClient."""

    def __init__(self, peer_id: str = "SELF_PEER_ID"):
        self.peer_id = peer_id
        self.calls: List[List[Tuple[str, Sequence[str]]]] = []
        self._ready_event: Optional[asyncio.Event] = None

    def set_ready_event(self, event: asyncio.Event) -> None:
        self._ready_event = event

    async def set_preferred_peers(
        self, peers: Sequence[Tuple[str, Sequence[str]]]
    ) -> Tuple[int, int]:
        self.calls.append(list(peers))
        if self._ready_event is not None:
            self._ready_event.set()
        return len(peers), 0


class FailOnceP2PClient(FakeP2PClient):
    """Raises RuntimeError on the first call, then succeeds."""

    def __init__(self, peer_id: str = "SELF_PEER_ID"):
        super().__init__(peer_id)
        self._first = True

    async def set_preferred_peers(
        self, peers: Sequence[Tuple[str, Sequence[str]]]
    ) -> Tuple[int, int]:
        if self._first:
            self._first = False
            raise RuntimeError("transient failure")
        return await super().set_preferred_peers(peers)


def _make_fake_session_factory(aggregate_content):
    """Return a session_factory whose sessions yield a stub aggregate."""

    class FakeAggregate:
        content = aggregate_content

    @contextmanager
    def session_factory():
        yield MagicMock()

    return session_factory


def _make_fake_config(cache_ttl: float = 300.0) -> MagicMock:
    cfg = MagicMock()
    cfg.aleph.corechannel.address.value = "0xCORE"
    cfg.aleph.corechannel.cache_ttl.value = cache_ttl
    return cfg


@pytest.mark.asyncio
async def test_job_pushes_peers_once(monkeypatch):
    """Job with one active node pushes exactly that peer."""
    peer_content = {"nodes": [{"multiaddress": MADDR_B, "status": "active"}]}

    class FakeAggregate:
        content = peer_content

    monkeypatch.setattr(
        "aleph.services.p2p.jobs.get_aggregate_by_key",
        lambda session, owner, key: FakeAggregate(),
    )

    ready = asyncio.Event()
    client = FakeP2PClient(peer_id="SELF")
    client.set_ready_event(ready)

    task = asyncio.create_task(
        refresh_preferred_peers_job(
            config=_make_fake_config(cache_ttl=60.0),
            session_factory=_make_fake_session_factory(peer_content),
            p2p_client=client,
        )
    )

    try:
        await asyncio.wait_for(ready.wait(), timeout=3.0)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert client.calls, "set_preferred_peers was never called"
    assert client.calls[0] == [(PEER_B, [MADDR_B])]


@pytest.mark.asyncio
async def test_job_skips_empty_aggregate(monkeypatch):
    """Job with no aggregate does not call set_preferred_peers."""
    monkeypatch.setattr(
        "aleph.services.p2p.jobs.get_aggregate_by_key",
        lambda session, owner, key: None,
    )

    client = FakeP2PClient()
    task = asyncio.create_task(
        refresh_preferred_peers_job(
            config=_make_fake_config(cache_ttl=0.05),
            session_factory=_make_fake_session_factory(None),
            p2p_client=client,
        )
    )

    # Let it loop at least twice with the tiny interval.
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert client.calls == [], "set_preferred_peers should not have been called"


@pytest.mark.asyncio
async def test_job_survives_rpc_failure(monkeypatch):
    """Job loops past a transient failure and eventually calls set_preferred_peers."""
    peer_content = {"nodes": [{"multiaddress": MADDR_A, "status": "active"}]}

    class FakeAggregate:
        content = peer_content

    monkeypatch.setattr(
        "aleph.services.p2p.jobs.get_aggregate_by_key",
        lambda session, owner, key: FakeAggregate(),
    )

    ready = asyncio.Event()
    client = FailOnceP2PClient(peer_id="SELF")
    client.set_ready_event(ready)

    task = asyncio.create_task(
        refresh_preferred_peers_job(
            config=_make_fake_config(cache_ttl=0.01),
            session_factory=_make_fake_session_factory(peer_content),
            p2p_client=client,
        )
    )

    try:
        await asyncio.wait_for(ready.wait(), timeout=3.0)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert client.calls, "set_preferred_peers should have been called after recovery"
    assert client.calls[0] == [(PEER_A, [MADDR_A])]
