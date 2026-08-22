from aleph.services.p2p.protocol import _BoundedSeenCache


def test_bounded_seen_cache_membership_and_fifo_eviction():
    """The pubsub dedup cache must give O(1) membership and evict oldest-first
    once it exceeds its bound (regression for the O(n) deque scan it replaced)."""
    cache = _BoundedSeenCache(maxlen=3)
    keys = [("sender", f"hash{i}", "sig") for i in range(4)]

    for key in keys[:3]:
        assert key not in cache
        cache.add(key)

    assert len(cache) == 3
    assert all(key in cache for key in keys[:3])

    # Adding a 4th key evicts the oldest (FIFO), keeping the bound.
    cache.add(keys[3])
    assert len(cache) == 3
    assert keys[0] not in cache  # evicted
    assert keys[3] in cache
    assert all(key in cache for key in keys[1:4])
