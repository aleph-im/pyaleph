"""S3StorageEngine against an in-process moto S3 server (no Docker, no DB)."""

import uuid
from typing import Any, AsyncIterator, Dict, Iterator, List

import boto3
import moto.settings
import pytest
import pytest_asyncio
from botocore.exceptions import ClientError
from moto.server import ThreadedMotoServer

from aleph.services.storage.s3_engine import S3StorageEngine

REGION = "garage"
ACCESS_KEY_ID = "test-access-key"
SECRET_ACCESS_KEY = "test-secret-key"


@pytest.fixture(scope="module")
def moto_endpoint() -> Iterator[str]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


def _boto_client(endpoint_url: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
    )


@pytest.fixture
def s3_client(moto_endpoint: str):
    return _boto_client(moto_endpoint)


def _create_bucket(s3_client, name: str) -> str:
    s3_client.create_bucket(
        Bucket=name, CreateBucketConfiguration={"LocationConstraint": REGION}
    )
    return name


@pytest.fixture
def bucket(s3_client) -> str:
    return _create_bucket(s3_client, f"aleph-{uuid.uuid4().hex[:12]}")


def _make_engine(endpoint_url: str, bucket: str, prefix: str = "", **overrides):
    kwargs: Dict[str, Any] = dict(
        endpoint_url=endpoint_url,
        region=REGION,
        bucket=bucket,
        prefix=prefix,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
        ca_bundle=None,
    )
    kwargs.update(overrides)
    return S3StorageEngine(**kwargs)


@pytest_asyncio.fixture
async def engine(moto_endpoint: str, bucket: str) -> AsyncIterator[S3StorageEngine]:
    s3_engine = _make_engine(moto_endpoint, bucket)
    try:
        yield s3_engine
    finally:
        await s3_engine.close()


@pytest.mark.asyncio
async def test_write_then_read_round_trip(engine: S3StorageEngine):
    await engine.write(filename="some-hash", content=b"hello aleph")
    assert await engine.read(filename="some-hash") == b"hello aleph"


@pytest.mark.asyncio
async def test_read_missing_returns_none(engine: S3StorageEngine):
    assert await engine.read(filename="does-not-exist") is None
    assert await engine.read_iterator(filename="does-not-exist") is None


@pytest.mark.asyncio
async def test_exists_true_false(engine: S3StorageEngine):
    assert await engine.exists(filename="file") is False
    await engine.write(filename="file", content=b"x")
    assert await engine.exists(filename="file") is True


@pytest.mark.asyncio
async def test_exists_raises_on_non_404(
    moto_endpoint: str, bucket: str, monkeypatch: pytest.MonkeyPatch
):
    """A rejected key (403) must surface, not read as "file missing".

    HeadObject has no response body, so S3 (and moto) answer a missing bucket
    and a missing key with the same bare 404; the missing bucket is caught by
    the bucket check instead (test_missing_bucket_raises). Here moto enforces
    authentication and the engine uses an unknown access key. The client
    (and its one-time bucket check) is created before authentication is
    switched on, so the 403 comes from HeadObject in exists().
    """
    s3_engine = _make_engine(
        moto_endpoint, bucket, access_key_id="AKIAUNKNOWNKEY000000"
    )
    try:
        await s3_engine._get_client()
        monkeypatch.setattr(moto.settings, "INITIAL_NO_AUTH_ACTION_COUNT", 0)
        with pytest.raises(ClientError) as excinfo:
            await s3_engine.exists(filename="file")
        assert excinfo.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    finally:
        await s3_engine.close()


@pytest.mark.asyncio
async def test_missing_bucket_raises(moto_endpoint: str):
    s3_engine = _make_engine(moto_endpoint, f"missing-{uuid.uuid4().hex[:12]}")
    try:
        with pytest.raises(ClientError):
            await s3_engine.exists(filename="file")
        with pytest.raises(ClientError):
            await s3_engine.read(filename="file")
    finally:
        await s3_engine.close()


@pytest.mark.asyncio
async def test_delete_is_idempotent(engine: S3StorageEngine):
    await engine.write(filename="file", content=b"x")
    await engine.delete(filename="file")
    assert await engine.exists(filename="file") is False
    # Deleting again (an object that does not exist) is not an error.
    await engine.delete(filename="file")


@pytest.mark.asyncio
async def test_read_iterator_streams_large_object(engine: S3StorageEngine):
    chunk_size = 1024 * 1024
    content = bytes(range(256)) * (8 * chunk_size // 256)
    await engine.write(filename="big", content=content)

    iterator = await engine.read_iterator(filename="big", chunk_size=chunk_size)
    assert iterator is not None
    chunks: List[bytes] = [chunk async for chunk in iterator]

    assert len(chunks) == 8
    assert all(len(chunk) == chunk_size for chunk in chunks)
    assert b"".join(chunks) == content


@pytest.mark.asyncio
async def test_read_iterator_stops_early(engine: S3StorageEngine):
    """A consumer that stops early closes the response; the engine still works."""
    await engine.write(filename="big", content=b"a" * (3 * 1024))
    iterator = await engine.read_iterator(filename="big", chunk_size=1024)
    assert iterator is not None
    async for chunk in iterator:
        assert chunk == b"a" * 1024
        break
    await iterator.aclose()  # type: ignore[attr-defined]
    assert await engine.read(filename="big") == b"a" * (3 * 1024)


@pytest.mark.asyncio
async def test_prefix_is_applied(moto_endpoint: str, bucket: str, s3_client):
    s3_engine = _make_engine(moto_endpoint, bucket, prefix="node-1/")
    try:
        await s3_engine.write(filename="abc", content=b"x")
        assert await s3_engine.read(filename="abc") == b"x"
    finally:
        await s3_engine.close()

    keys = [obj["Key"] for obj in s3_client.list_objects_v2(Bucket=bucket)["Contents"]]
    assert keys == ["node-1/abc"]


@pytest.mark.asyncio
async def test_path_style_addressing(moto_endpoint: str, s3_client):
    # A dotted bucket name behind a hostname endpoint: virtual-hosted style
    # would send the request to http://aleph.files.localhost:<port>/key.
    dotted_bucket = _create_bucket(s3_client, f"aleph.files.{uuid.uuid4().hex[:8]}")
    localhost_endpoint = moto_endpoint.replace("127.0.0.1", "localhost")
    s3_engine = _make_engine(localhost_endpoint, dotted_bucket)
    urls: List[str] = []
    try:
        client = await s3_engine._get_client()
        assert client.meta.config.s3["addressing_style"] == "path"
        client.meta.events.register(
            "before-send.s3", lambda request, **_: urls.append(request.url)
        )
        await s3_engine.write(filename="abc", content=b"x")
        assert await s3_engine.read(filename="abc") == b"x"
    finally:
        await s3_engine.close()

    assert urls
    for url in urls:
        assert url.startswith(f"{localhost_endpoint}/{dotted_bucket}/"), url


def test_client_config_retries_and_path_style():
    s3_engine = _make_engine("https://s3.garage.svc", "bucket")
    client_config = s3_engine._client_config
    assert client_config.s3 == {"addressing_style": "path"}
    assert client_config.retries == {"max_attempts": 5, "mode": "standard"}
    assert client_config.connect_timeout == 5
    assert client_config.read_timeout == 60
