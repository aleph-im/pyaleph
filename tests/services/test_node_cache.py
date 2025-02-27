import pytest

from aleph.services.cache.node_cache import NodeCache


@pytest.mark.asyncio
async def test_get_set(node_cache: NodeCache):
    key = "test_get_set"

    await node_cache.set(key, 12)
    assert await node_cache.get(key) == b"12"

    await node_cache.set(key, "hello")
    assert await node_cache.get(key) == b"hello"


@pytest.mark.asyncio
async def test_incr_decr(node_cache: NodeCache):
    key = "test_incr_decr"
    await node_cache.redis_client.delete(key)

    await node_cache.set(key, 42)
    await node_cache.incr(key)
    assert await node_cache.get(key) == b"43"

    await node_cache.decr(key)
    assert await node_cache.get(key) == b"42"


@pytest.mark.asyncio
async def test_api_servers_cache(node_cache: NodeCache):
    await node_cache.redis_client.delete(node_cache.API_SERVERS_KEY)

    assert await node_cache.get_api_servers() == set()

    api_server_1 = "https://api2.aleph.im"
    api_server_2 = "https://api3.aleph.im"

    await node_cache.add_api_server(api_server_2)
    assert await node_cache.get_api_servers() == {api_server_2}
    assert not await node_cache.has_api_server(api_server_1)
    assert await node_cache.has_api_server(api_server_2)

    await node_cache.add_api_server(api_server_1)
    assert await node_cache.get_api_servers() == {api_server_1, api_server_2}
    assert await node_cache.has_api_server(api_server_1)
    assert await node_cache.has_api_server(api_server_2)

    await node_cache.redis_client.delete(node_cache.API_SERVERS_KEY)
