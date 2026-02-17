from hashlib import sha256
from typing import Any, Dict, List, Optional, Set

import redis.asyncio as redis_asyncio

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.messages import (
    count_matching_messages,
    count_matching_messages_fast,
)
from aleph.schemas.messages_query_params import MessageQueryParams
from aleph.types.db_session import DbSessionFactory

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

    @staticmethod
    def _try_fast_count(
        session_factory: DbSessionFactory, query_params: MessageQueryParams
    ) -> Optional[int]:
        """
        Try an O(1) lookup from message_counts for simple queries.
        Returns None if the query has filters that cannot be answered
        by the counter table (date ranges, hashes, refs, etc.).
        """
        # Fast path only works when no non-dimension filters are set
        if (
            query_params.hashes
            or query_params.refs
            or query_params.content_hashes
            or query_params.content_keys
            or query_params.content_types
            or query_params.chains
            or query_params.channels
            or query_params.tags
            or query_params.payment_types
            or query_params.start_date
            or query_params.end_date
            or query_params.start_block
            or query_params.end_block
        ):
            return None

        # Fast path only supports single-value dimensions
        if query_params.addresses and len(query_params.addresses) > 1:
            return None
        if query_params.owners and len(query_params.owners) > 1:
            return None
        message_types = query_params.message_types or (
            [query_params.message_type] if query_params.message_type else None
        )
        if message_types and len(message_types) > 1:
            return None

        sender = query_params.addresses[0] if query_params.addresses else None
        owner = query_params.owners[0] if query_params.owners else None
        msg_type = message_types[0].value if message_types else None
        statuses = query_params.message_statuses or []

        with session_factory() as session:
            total = 0
            for status in statuses:
                result = count_matching_messages_fast(
                    session,
                    message_type=msg_type,
                    status=status.value,
                    sender=sender,
                    owner=owner,
                )
                if result is None:
                    return None
                total += result
            # If no statuses filter, query with no status filter
            if not statuses:
                result = count_matching_messages_fast(
                    session, message_type=msg_type, sender=sender, owner=owner
                )
                if result is None:
                    return None
                total = result
        return total

    async def count_messages(
        self, session_factory: DbSessionFactory, query_params: MessageQueryParams
    ) -> int:
        # Try O(1) lookup from message_counts table
        fast_result = self._try_fast_count(session_factory, query_params)
        if fast_result is not None:
            return fast_result

        # Fall back to Redis-cached COUNT(*)
        filters = query_params.model_dump(exclude_none=True)
        cache_key = f"message_count:{self._message_filter_id(filters)}"

        cached_result = await self.get(cache_key)
        if cached_result is not None:
            return int(cached_result.decode())

        # Slow, can take a few seconds
        with session_factory() as session:
            n_matches = count_matching_messages(session, **filters)

        await self.set(cache_key, n_matches, expiration=self.message_cache_count_ttl)

        return n_matches
