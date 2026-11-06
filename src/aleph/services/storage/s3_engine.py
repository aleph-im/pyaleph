"""Storage engine keeping files in an S3-compatible bucket (Garage, MinIO, AWS).

Objects are stored under ``<prefix><filename>`` with path-style addressing, so
any bucket name works behind a single endpoint hostname (no wildcard DNS or
certificate needed). One aiobotocore client is created lazily per engine and
released by :meth:`S3StorageEngine.close`.
"""

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any, AsyncIterable, AsyncIterator, Optional

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

from .engine import StorageEngine

LOGGER = logging.getLogger(__name__)


def _is_missing_key(error: ClientError) -> bool:
    """Whether a ClientError means "no such object" (and nothing worse).

    HeadObject answers with a bare 404 (no body, so no error code); GetObject
    says NoSuchKey. A missing bucket must not read as a missing file.
    """
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = error.response.get("Error", {}).get("Code")
    return status == 404 and code != "NoSuchBucket"


class S3StorageEngine(StorageEngine):
    def __init__(
        self,
        *,
        endpoint_url: str,
        region: str,
        bucket: str,
        prefix: str,
        access_key_id: str,
        secret_access_key: str,
        ca_bundle: Optional[str],
    ):
        self.endpoint_url = endpoint_url
        self.region = region
        self.bucket = bucket
        self.prefix = prefix
        self.ca_bundle = ca_bundle
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key

        self._client_config = AioConfig(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=5,
            read_timeout=60,
        )
        self._session = get_session()
        self._client: Any = None
        self._exit_stack: Optional[AsyncExitStack] = None
        self._client_lock = asyncio.Lock()

    def _key(self, filename: str) -> str:
        return f"{self.prefix}{filename}"

    async def _get_client(self) -> Any:
        """The S3 client, created on first use.

        The bucket is checked once when the client is created, so a missing
        bucket or rejected credentials fail loudly instead of every file
        looking absent.
        """
        if self._client is not None:
            return self._client

        async with self._client_lock:
            if self._client is None:
                client_kwargs: dict = {}
                if self.ca_bundle:
                    client_kwargs["verify"] = self.ca_bundle

                exit_stack = AsyncExitStack()
                try:
                    client = await exit_stack.enter_async_context(
                        self._session.create_client(
                            "s3",
                            endpoint_url=self.endpoint_url,
                            region_name=self.region,
                            aws_access_key_id=self._access_key_id,
                            aws_secret_access_key=self._secret_access_key,
                            config=self._client_config,
                            **client_kwargs,
                        )
                    )
                    await client.head_bucket(Bucket=self.bucket)
                except BaseException:
                    await exit_stack.aclose()
                    raise

                LOGGER.info(
                    "S3 storage engine using bucket %s at %s",
                    self.bucket,
                    self.endpoint_url,
                )
                self._exit_stack = exit_stack
                self._client = client

        return self._client

    async def close(self) -> None:
        async with self._client_lock:
            exit_stack, self._exit_stack, self._client = self._exit_stack, None, None
            if exit_stack is not None:
                await exit_stack.aclose()

    async def read(self, filename: str) -> Optional[bytes]:
        client = await self._get_client()
        try:
            response = await client.get_object(
                Bucket=self.bucket, Key=self._key(filename)
            )
        except ClientError as error:
            if _is_missing_key(error):
                return None
            raise

        async with response["Body"] as body:
            return await body.read()

    async def read_iterator(
        self, filename: str, chunk_size: int = 1024 * 1024
    ) -> Optional[AsyncIterable[bytes]]:
        client = await self._get_client()
        try:
            response = await client.get_object(
                Bucket=self.bucket, Key=self._key(filename)
            )
        except ClientError as error:
            if _is_missing_key(error):
                return None
            raise

        async def _read_iterator(streaming_body) -> AsyncIterator[bytes]:
            # `async with` releases the HTTP response even when the consumer
            # stops early (the generator is closed at the yield).
            async with streaming_body as body:
                buffer = bytearray()
                while True:
                    data = await body.read(chunk_size - len(buffer))
                    if not data:
                        break
                    buffer.extend(data)
                    if len(buffer) >= chunk_size:
                        yield bytes(buffer)
                        buffer.clear()
                if buffer:
                    yield bytes(buffer)

        return _read_iterator(response["Body"])

    async def write(self, filename: str, content: bytes):
        client = await self._get_client()
        await client.put_object(
            Bucket=self.bucket, Key=self._key(filename), Body=content
        )

    async def delete(self, filename: str):
        client = await self._get_client()
        # DeleteObject succeeds on a missing key, so delete is idempotent.
        await client.delete_object(Bucket=self.bucket, Key=self._key(filename))

    async def exists(self, filename: str) -> bool:
        client = await self._get_client()
        try:
            await client.head_object(Bucket=self.bucket, Key=self._key(filename))
        except ClientError as error:
            if _is_missing_key(error):
                return False
            raise
        return True
