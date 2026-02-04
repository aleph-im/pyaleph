"""Storage module for Aleph.
Basically manages the IPFS storage.
"""

import asyncio
import json
import logging
from hashlib import sha256
from typing import Any, Final, Optional, cast

from aleph_message.models import ItemType

import aleph.toolkit.json as aleph_json
from aleph.config import get_config
from aleph.db.accessors.files import upsert_file
from aleph.db.models.pending_messages import PendingMessageDb
from aleph.exceptions import ContentCurrentlyUnavailable, InvalidContent
from aleph.schemas.base_messages import AlephBaseMessage
from aleph.schemas.message_content import (
    ContentSource,
    MessageContent,
    RawContent,
    StreamContent,
)
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import get_cid_version
from aleph.services.p2p.http import request_hash as p2p_http_request_hash
from aleph.services.storage.engine import StorageEngine
from aleph.types.db_session import DbSession
from aleph.types.files import FileType
from aleph.utils import get_sha256

LOGGER = logging.getLogger(__name__)


U0000_STR: Final = "\\u0000"
U0000_BYTES: Final = U0000_STR.encode("utf-8")
STREAM_CHUNK_SIZE: Final = 128 * 1024


def check_for_u0000(item_content: aleph_json.SerializedJsonInput):
    # Note: this condition is a bit longer than it should be to make it clear
    #       for mypy that we are comparing the correct types.
    if isinstance(item_content, str):
        contains_u0000 = U0000_STR in item_content
    else:
        contains_u0000 = U0000_BYTES in item_content

    if contains_u0000:
        error_msg = "Unsupported character in message: \\u0000"
        LOGGER.warning(error_msg)
        raise InvalidContent(error_msg)


class StorageService:
    def __init__(
        self,
        storage_engine: StorageEngine,
        ipfs_service: IpfsService,
        node_cache: NodeCache,
    ):
        self.storage_engine = storage_engine
        self.ipfs_service = ipfs_service
        self.node_cache = node_cache

    async def get_message_content(
        self, message: AlephBaseMessage | PendingMessageDb
    ) -> MessageContent:
        item_type = message.item_type
        item_hash = message.item_hash

        item_content: aleph_json.SerializedJsonInput

        if item_type in (ItemType.ipfs, ItemType.storage):
            hash_content = await self.get_hash_content(
                item_hash, engine=ItemType(item_type)
            )
            item_content = hash_content.value
            source = hash_content.source
        elif item_type == ItemType.inline:
            # This hypothesis is validated at schema level
            item_content = cast(str, message.item_content)
            source = ContentSource.INLINE
        else:
            # unknown, could retry later? shouldn't have arrived this far though.
            raise ValueError(f"Unknown item type: '{item_type}'.")

        check_for_u0000(item_content)

        try:
            content = aleph_json.loads(item_content)
        except aleph_json.DecodeError as e:
            error_msg = f"Can't decode JSON: {e}"
            LOGGER.warning(error_msg)
            raise InvalidContent(error_msg)
        except json.decoder.JSONDecodeError as e:
            error_msg = f"Can't decode JSON: {e}"
            LOGGER.warning(error_msg)
            raise InvalidContent(error_msg)

        return MessageContent(
            hash=item_hash,
            source=source,
            value=content,
            raw_value=item_content,
        )

    async def _fetch_content_from_network(
        self, content_hash: str, engine: ItemType, timeout: int
    ) -> Optional[bytes]:
        content = None
        config = get_config()
        enabled_clients = config.p2p.clients.value

        if "http" in enabled_clients:
            api_servers = list(await self.node_cache.get_api_servers())
            content = await p2p_http_request_hash(
                api_servers=api_servers, item_hash=content_hash, timeout=timeout
            )

        if content is not None:
            await self._verify_content_hash(content, engine, content_hash)

        return content

    async def _compute_content_hash_ipfs(
        self, content: bytes, cid_version: int = 1
    ) -> Optional[str]:
        """
        Computes the IPFS hash of the content.
        :param content: Content to hash.
        :param cid_version: CID version of the hash.
        :return: The computed hash of the content. Can return None if the operation fails for some reason.
        """

        # TODO: get a better way to compare hashes (without depending on the IPFS daemon)
        try:
            computed_hash = await self.ipfs_service.add_bytes(
                content, cid_version=cid_version
            )
            return computed_hash

        except asyncio.TimeoutError:
            LOGGER.warning("Timeout while computing IPFS hash.")
            return None

    @staticmethod
    async def _compute_content_hash_sha256(content: bytes) -> str:
        return get_sha256(content)

    async def _verify_content_hash(
        self, content: bytes, engine: ItemType, expected_hash: str
    ) -> None:
        """
        Checks that the hash of a content we fetched from the network matches the expected hash.
        Raises an exception if the content does not match the expected hash.
        :raises InvalidContent: The computed hash does not match.
        :raises ContentCurrentlyUnavailable: The hash cannot be computed at this time.
        """
        config = get_config()
        ipfs_enabled = config.ipfs.enabled.value

        if engine == ItemType.ipfs and ipfs_enabled:
            try:
                cid_version = get_cid_version(expected_hash)
            except ValueError as e:
                raise InvalidContent(e) from e
            compute_hash_task = self._compute_content_hash_ipfs(content, cid_version)
        elif engine == ItemType.storage:
            compute_hash_task = self._compute_content_hash_sha256(content)
        else:
            raise ValueError(f"Invalid storage engine: '{engine}'.")

        computed_hash = await compute_hash_task

        if computed_hash is None:
            error_msg = f"Could not compute hash for '{expected_hash}'."
            LOGGER.warning(error_msg)
            raise ContentCurrentlyUnavailable(error_msg)

        if computed_hash != expected_hash:
            error_msg = f"Got a bad hash! Expected '{expected_hash}' but computed '{computed_hash}'."
            LOGGER.warning(error_msg)
            raise InvalidContent(error_msg)

    async def get_hash_content(
        self,
        content_hash: str,
        engine: ItemType = ItemType.ipfs,
        timeout: int = 2,
        tries: int = 1,
        use_network: bool = True,
        use_ipfs: bool = True,
        store_value: bool = True,
    ) -> RawContent:
        # TODO: determine which storage engine to use

        source = None

        # Try to retrieve the data from the DB, then from the network or IPFS.
        content = await self.storage_engine.read(filename=content_hash)
        if content is not None:
            source = ContentSource.DB

        if content is None and use_network:
            content = await self._fetch_content_from_network(
                content_hash, engine, timeout
            )
            source = ContentSource.P2P

        if content is None:
            if use_ipfs and engine == ItemType.ipfs:
                config = get_config()
                ipfs_enabled = config.ipfs.enabled.value
                if ipfs_enabled:
                    content = await self.ipfs_service.get_ipfs_content(
                        content_hash, timeout=timeout, tries=tries
                    )
                    source = ContentSource.IPFS

        if content is None:
            raise ContentCurrentlyUnavailable(
                f"Could not fetch content for '{content_hash}'."
            )

        LOGGER.info("Got content from %s for '%s'.", source.value, content_hash)  # type: ignore

        # Store content locally if we fetched it from the network
        if store_value and source != ContentSource.DB:
            LOGGER.debug(f"Storing content for '{content_hash}'.")
            await self.storage_engine.write(filename=content_hash, content=content)

        return RawContent(hash=content_hash, value=content, source=source)

    async def get_hash_content_iterator(
        self,
        content_hash: str,
        engine: ItemType = ItemType.ipfs,
        timeout: int = 2,
        tries: int = 1,
        use_network: bool = True,
        use_ipfs: bool = True,
    ) -> StreamContent:
        # Try to retrieve the data from the DB, then from IPFS.
        # P2P retrieval via HTTP does not easily support streaming yet in this codebase
        # as it fetches JSON with base64 content.

        source = None

        content_iterator = await self.storage_engine.read_iterator(
            filename=content_hash, chunk_size=STREAM_CHUNK_SIZE
        )
        if content_iterator is not None:
            source = ContentSource.DB

        if content_iterator is None:
            if use_ipfs and engine == ItemType.ipfs:
                config = get_config()
                ipfs_enabled = config.ipfs.enabled.value
                if ipfs_enabled:
                    content_iterator = (
                        await self.ipfs_service.get_ipfs_content_iterator(
                            content_hash, chunk_size=STREAM_CHUNK_SIZE
                        )
                    )
                    source = ContentSource.IPFS

        if content_iterator is None:
            raise ContentCurrentlyUnavailable(
                f"Could not fetch content for '{content_hash}'."
            )

        return StreamContent(hash=content_hash, value=content_iterator, source=source)

    async def get_json(
        self, content_hash: str, engine=ItemType.ipfs, timeout: int = 2, tries: int = 1
    ) -> MessageContent:
        content = await self.get_hash_content(
            content_hash, engine=engine, timeout=timeout, tries=tries
        )

        try:
            json_content = aleph_json.loads(content.value)
        except aleph_json.DecodeError as e:
            LOGGER.exception("Can't decode JSON")
            raise InvalidContent("Cannot decode JSON") from e

        return MessageContent(
            hash=content.hash,
            value=json_content,
            source=content.source,
            raw_value=content.value,
        )

    async def pin_hash(self, chash: str, timeout: int = 30, tries: int = 1):
        await self.ipfs_service.pin_add(cid=chash, timeout=timeout, tries=tries)

    async def add_json(
        self, session: DbSession, value: Any, engine: ItemType = ItemType.ipfs
    ) -> str:
        content = aleph_json.dumps(value)

        if engine == ItemType.ipfs:
            chash = await self.ipfs_service.add_bytes(content)
        elif engine == ItemType.storage:
            chash = sha256(content).hexdigest()
        else:
            raise NotImplementedError("storage engine %s not supported" % engine)

        await self.storage_engine.write(filename=chash, content=content)
        upsert_file(
            session=session,
            file_hash=chash,
            size=len(content),
            file_type=FileType.FILE,
        )

        return chash

    async def add_file_content_to_local_storage(
        self, session: DbSession, file_content: bytes, file_hash: str
    ) -> None:
        await self.storage_engine.write(filename=file_hash, content=file_content)
        upsert_file(
            session=session,
            file_hash=file_hash,
            size=len(file_content),
            file_type=FileType.FILE,
        )

    async def add_file(
        self, session: DbSession, file_content: bytes, engine: ItemType = ItemType.ipfs
    ) -> str:
        if engine == ItemType.ipfs:
            file_hash = await self.ipfs_service.add_bytes(file_content)

        elif engine == ItemType.storage:
            file_hash = sha256(file_content).hexdigest()

        else:
            raise ValueError(f"Unsupported item type: {engine}")

        await self.add_file_content_to_local_storage(
            session=session, file_content=file_content, file_hash=file_hash
        )

        return file_hash
