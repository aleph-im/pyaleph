""" Storage module for Aleph.
Basically manages the IPFS storage.
"""
import asyncio
import json
import logging
from hashlib import sha256
from typing import Any, AnyStr, IO, Optional, cast

from aleph_message.models import ItemType

from aleph.config import get_config
from aleph.exceptions import InvalidContent, ContentCurrentlyUnavailable
from aleph.schemas.message_content import ContentSource, RawContent, MessageContent
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.services.ipfs.common import get_cid_version
from aleph.services.ipfs import IpfsService
from aleph.services.p2p.http import request_hash as p2p_http_request_hash
from aleph.services.storage.engine import StorageEngine
from aleph.utils import get_sha256, run_in_executor

LOGGER = logging.getLogger("STORAGE")


async def json_async_loads(s: AnyStr):
    """Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance
    containing a JSON document) to a Python object in an asynchronous executor."""
    return await run_in_executor(None, json.loads, s)


class StorageService:
    def __init__(self, storage_engine: StorageEngine, ipfs_service: IpfsService):
        self.storage_engine = storage_engine
        self.ipfs_service = ipfs_service

    async def get_message_content(self, message: BasePendingMessage) -> MessageContent:
        item_type = message.item_type
        item_hash = message.item_hash

        if item_type in (ItemType.ipfs, ItemType.storage):
            return await self.get_json(item_hash, engine=ItemType(item_type))
        elif item_type == ItemType.inline:
            # This hypothesis is validated at schema level
            item_content = cast(str, message.item_content)

            try:
                content = await json_async_loads(item_content)
            except (json.JSONDecodeError, KeyError) as e:
                error_msg = f"Can't decode JSON: {e}"
                LOGGER.warning(error_msg)
                raise InvalidContent(error_msg)

            return MessageContent(
                hash=item_hash,
                source=ContentSource.INLINE,
                value=content,
                raw_value=item_content,
            )
        else:
            # unknown, could retry later? shouldn't have arrived this far though.
            error_msg = f"Unknown item type: '{item_type}'."
            raise ContentCurrentlyUnavailable(error_msg)

    async def _fetch_content_from_network(
        self, content_hash: str, engine: ItemType, timeout: int
    ) -> Optional[bytes]:
        content = None
        config = get_config()
        enabled_clients = config.p2p.clients.value

        if "http" in enabled_clients:
            content = await p2p_http_request_hash(content_hash, timeout=timeout)

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
            LOGGER.warning(f"Timeout while computing IPFS hash.")
            return None

    @staticmethod
    async def _compute_content_hash_sha256(content: bytes) -> str:
        return get_sha256(content)

    async def _verify_content_hash(
        self, content: bytes, engine: ItemType, expected_hash: str
    ) -> None:
        """
        Checks that the hash of a content we fetched from the network matches the expected hash.
        :return: True if the hashes match, False otherwise.
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
        config = get_config()
        ipfs_enabled = config.ipfs.enabled.value

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
            if ipfs_enabled and engine == ItemType.ipfs and use_ipfs:
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

    async def get_json(
        self, content_hash: str, engine=ItemType.ipfs, timeout: int = 2, tries: int = 1
    ) -> MessageContent:
        content = await self.get_hash_content(
            content_hash, engine=engine, timeout=timeout, tries=tries
        )

        try:
            json_content = await json_async_loads(content.value)
        except json.JSONDecodeError as e:
            LOGGER.exception("Can't decode JSON")
            raise InvalidContent("Cannot decode JSON") from e

        return MessageContent(
            hash=content.hash,
            value=json_content,
            source=content.source,
            raw_value=content.value,
        )

    async def pin_hash(self, chash: str, timeout: int = 2, tries: int = 1):
        return await self.ipfs_service.pin_add(chash, timeout=timeout, tries=tries)

    async def add_json(self, value: Any, engine: ItemType = ItemType.ipfs) -> str:
        # TODO: determine which storage engine to use
        content = await run_in_executor(None, json.dumps, value)
        content = content.encode("utf-8")
        if engine == ItemType.ipfs:
            chash = await self.ipfs_service.add_bytes(content)
        elif engine == ItemType.storage:
            if isinstance(content, str):
                content = content.encode("utf-8")
            chash = sha256(content).hexdigest()
        else:
            raise NotImplementedError("storage engine %s not supported" % engine)

        await self.storage_engine.write(filename=chash, content=content)
        return chash

    async def add_file(self, fileobject: IO, engine: ItemType = ItemType.ipfs) -> str:

        if engine == ItemType.ipfs:
            output = await self.ipfs_service.add_file(fileobject)
            file_hash = output["Hash"]
            fileobject.seek(0)
            file_content = fileobject.read()

        elif engine == ItemType.storage:
            file_content = fileobject.read()
            file_hash = sha256(file_content).hexdigest()

        else:
            raise ValueError(f"Unsupported item type: {engine}")

        await self.storage_engine.write(filename=file_hash, content=file_content)
        return file_hash
