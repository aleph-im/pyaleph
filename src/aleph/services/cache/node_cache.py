from typing import Any, Set, Optional, List

import redis.asyncio as redis_asyncio

CacheKey = Any
CacheValue = bytes


class NodeCache:
    API_SERVERS_KEY = "api_servers"
    PUBLIC_ADDRESSES_KEY = "public_addresses"

    def __init__(self, redis_host: str, redis_port: int):
        self.redis_host = redis_host
        self.redis_port = redis_port

        self._redis_client: Optional[redis_asyncio.Redis] = None


    @property
    def redis_client(self) -> redis_asyncio.Redis:
        if (redis_client := self._redis_client) is None:
            raise ValueError(
                "Redis client must be initialized. "
                f"Call open() first or use `async with {self.__class__.__name__}()`."
            )

        return redis_client


    async def open(self):
        self._redis_client = redis_asyncio.Redis(
            host=self.redis_host, port=self.redis_port
        )

    async def __aenter__(self):
        await self.open()
        return self

    async def close(self):
        if self.redis_client:
            await self.redis_client.aclose()
            self._redis_client = None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def reset(self):
        """
        Resets the cache to sane defaults after a reboot of the node.
        """
        await self.redis_client.delete(self.PUBLIC_ADDRESSES_KEY)

    async def get(self, key: CacheKey) -> Optional[CacheValue]:
        return await self.redis_client.get(key)

    async def set(self, key: CacheKey, value: Any):
        await self.redis_client.set(key, value)

    async def incr(self, key: CacheKey):
        await self.redis_client.incr(key)

    async def decr(self, key: CacheKey):
        await self.redis_client.decr(key)

    async def get_api_servers(self) -> Set[str]:
        return set(
            api_server.decode()
            for api_server in await self.redis_client.smembers(self.API_SERVERS_KEY)
        )

    async def add_api_server(self, api_server: str) -> None:
        await self.redis_client.sadd(self.API_SERVERS_KEY, api_server)

    async def has_api_server(self, api_server: str) -> bool:
        return await self.redis_client.sismember(self.API_SERVERS_KEY, api_server)

    async def remove_api_server(self, api_server: str) -> None:
        await self.redis_client.srem(self.API_SERVERS_KEY, api_server)

    async def add_public_address(self, public_address: str) -> None:
        await self.redis_client.sadd(self.PUBLIC_ADDRESSES_KEY, public_address)

    async def get_public_addresses(self) -> List[str]:
        addresses = await self.redis_client.smembers(self.PUBLIC_ADDRESSES_KEY)
        return [addr.decode() for addr in addresses]
