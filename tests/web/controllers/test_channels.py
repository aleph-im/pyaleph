from unittest.mock import Mock, patch

import pytest

from aleph.web.controllers import channels as channels_module


@pytest.mark.asyncio
async def test_get_channels_caches_across_sessions():
    """The channel-list cache must key on a constant, not the per-request
    Session.

    Regression for a memory leak: aiocache's default key builder includes the
    Session argument's repr (its memory address), so every request produced a
    distinct key -- the cache never hit and grew one entry per request until TTL
    eviction. With a constant key, two calls made with different Session objects
    must resolve to a single underlying query.
    """
    await channels_module.get_channels.cache.clear()

    calls = {"n": 0}

    def _fake_distinct(session):
        calls["n"] += 1
        return ["chanA", None, "chanB"]

    try:
        with patch.object(channels_module, "get_distinct_channels", _fake_distinct):
            first = await channels_module.get_channels(Mock(name="session-1"))
            second = await channels_module.get_channels(Mock(name="session-2"))

        assert first == ["chanA", "chanB"]  # None filtered out
        assert second == ["chanA", "chanB"]
        # A per-Session key would call the DB twice (and cache two entries);
        # the constant key hits the cache on the second, distinct Session.
        assert calls["n"] == 1
    finally:
        await channels_module.get_channels.cache.clear()
