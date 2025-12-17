from hashlib import sha256
from typing import Any, Dict, List, Optional, Set

import redis.asyncio as redis_asyncio
from sqlalchemy import func, select

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.address import make_fetch_stats_address_query
from aleph.db.accessors.messages import count_matching_messages
from aleph.schemas.messages_query_params import MessageQueryParams
from aleph.types.db_session import DbSession

CacheKey = Any
CacheValue = bytes


class NodeCache:
    API_SERVERS_KEY = "api_servers"
    PUBLIC_ADDRESSES_KEY = "public_addresses"

    def __init__(self, redis_host: str, redis_port: int, message_count_cache_ttl):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.message_cache_count_ttl = message_count_cache_ttl

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

    async def set(self, key: CacheKey, value: Any, expiration: Optional[int] = None):
        await self.redis_client.set(key, value, ex=expiration)

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

    @staticmethod
    def _message_filter_id(filters: Dict[str, Any]):
        filters_json = aleph_json.dumps(filters, sort_keys=True)
        return sha256(filters_json).hexdigest()

    async def count_messages(
        self, session: DbSession, query_params: MessageQueryParams
    ) -> int:
        filters = query_params.model_dump(exclude_none=True)
        cache_key = f"message_count:{self._message_filter_id(filters)}"

        cached_result = await self.get(cache_key)
        if cached_result is not None:
            return int(cached_result.decode())

        # Slow, can take a few seconds
        n_matches = count_matching_messages(session, **filters)

        await self.set(cache_key, n_matches, expiration=self.message_cache_count_ttl)

        return n_matches

    async def count_address_stats(
        self,
        session: DbSession,
        filters: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Count matching address stats (grouped by address),
        with Redis caching.
        """
        # Use empty dict if filters is None
        filters_dict = {} if filters is None else filters
        cache_key = f"address_stats_count:{self._message_filter_id(filters_dict)}"

        cached_result = await self.get(cache_key)
        if cached_result is not None:
            return int(cached_result.decode())

        # Slow query: count grouped addresses
        # Pass only non-None filters to the query
        query_args = {} if filters is None else filters
        stmt = make_fetch_stats_address_query(**query_args)
        count_stmt = select(func.count()).select_from(stmt.subquery())

        total = session.execute(count_stmt).scalar_one()

        await self.set(
            cache_key,
            total,
            expiration=self.message_cache_count_ttl,
        )

        return total
