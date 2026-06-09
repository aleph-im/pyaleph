import asyncio
import base64
import concurrent
import datetime as dt
import json
import logging
import pathlib
import tempfile
from dataclasses import dataclass
from typing import AsyncIterable, Dict, Optional, Self, Union, cast

import aiofiles
import aiohttp
import aioipfs
from configmanager import Config

from aleph.services.ipfs.common import make_ipfs_p2p_client, make_ipfs_pinning_client
from aleph.services.utils import get_IP
from aleph.types.message_status import FileUnavailable
from aleph.utils import run_in_executor

LOGGER = logging.getLogger(__name__)

MAX_LEN = 1024 * 1024 * 100


class InvalidIpnsRecordError(Exception):
    """The IPNS record failed verification against its name."""


class IpnsResolutionError(Exception):
    """The IPNS name could not be resolved from the DHT."""


@dataclass(frozen=True)
class IpnsRecordInfo:
    value_cid: str
    sequence: int
    validity: dt.datetime


class DagImportError(Exception):
    """kubo /api/v0/dag/import reported a failure (transport, malformed
    response, or a non-empty PinErrorMsg for an imported root)."""


def parse_dag_import_response(body: bytes) -> list[str]:
    """Walk the NDJSON body from kubo /api/v0/dag/import and return imported
    root CIDs in encounter order.

    Raises DagImportError on malformed JSON, missing fields, or any root
    with a non-empty PinErrorMsg.
    """
    roots: list[str] = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as e:
            raise DagImportError(f"malformed NDJSON line: {e}") from e
        root = entry.get("Root")
        if root is None:
            continue
        pin_err = root.get("PinErrorMsg") or ""
        if pin_err:
            raise DagImportError(f"kubo pin error: {pin_err}")
        cid = root.get("Cid", {}).get("/")
        if not cid:
            raise DagImportError(f"malformed Root entry (missing Cid./): {entry!r}")
        roots.append(cid)
    return roots


async def _fetch_ipfs_endpoint_streamed(
    aioipfs_client: aioipfs.AsyncIPFS,
    endpoint: str,
    params: Optional[Dict] = None,
    chunk_size: int = 16 * 1024,
) -> AsyncIterable[bytes]:
    """Stream content from an IPFS HTTP API endpoint."""
    driver = aioipfs_client.core.driver
    params = params or {}

    async with driver.session.post(
        aioipfs_client.core.url(endpoint), params=params, auth=driver.auth
    ) as response:
        if response.status in aioipfs.apis.HTTP_ERROR_CODES:
            data = await response.read()
            aioipfs_client.core.handle_error(response, data)

        async for chunk in response.content.iter_chunked(chunk_size):
            yield chunk


class IpfsService:
    def __init__(
        self,
        ipfs_client: aioipfs.AsyncIPFS,
        pinning_client: Optional[aioipfs.AsyncIPFS] = None,
    ):
        # P2P/network role only: swarm, identify, pubsub, peering.
        self.ipfs_client = ipfs_client
        # The separate storage daemon configured via `ipfs.pinning.*`, or None
        # when none is configured. Do NOT use this directly for storage
        # operations: it is None in single-daemon deployments. Route every
        # storage-role call through `storage_client` instead; this attribute
        # only holds the raw config state and is closed in `close`.
        self.pinning_client = pinning_client

    @property
    def storage_client(self) -> aioipfs.AsyncIPFS:
        """Client for storage-role operations: pin/unpin, content reads, file
        stats, dag/block ops.

        Returns the separate pinning daemon when one is configured
        (`ipfs.pinning.*`), otherwise falls back to the main daemon so
        single-daemon deployments need no extra config.
        """
        return self.pinning_client or self.ipfs_client

    @classmethod
    def new(cls, config: Config) -> Self:
        # Create P2P client (for content retrieval, pubsub)
        p2p_client = make_ipfs_p2p_client(config)

        # Build a separate pinning client only when an external pinning service
        # is configured; otherwise leave it None so `storage_client` falls back
        # to the P2P client and single-daemon deployments need no extra config.
        if _should_use_separate_pinning_client(config):
            LOGGER.info("Using separate IPFS client for storage operations")
            pinning_client = make_ipfs_pinning_client(config)
        else:
            pinning_client = None

        return cls(ipfs_client=p2p_client, pinning_client=pinning_client)

    async def __aenter__(self):
        return self

    async def close(self):
        await self.ipfs_client.close()
        # Only close the pinning client if a separate daemon was configured.
        if self.pinning_client is not None:
            await self.pinning_client.close()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self, peer: str) -> Dict:
        return await self.ipfs_client.swarm.connect(peer)

    async def get_public_address(self):
        public_ip = await get_IP()

        addresses = (await self.ipfs_client.id())["Addresses"]
        for address in addresses:
            if public_ip in address and "/tcp" in address and "/p2p" in address:
                return address

        # Fallback to first possible public...
        for address in addresses:
            if "127.0.0.1" not in address and "/tcp" in address and "/p2p" in address:
                return address

        # Still no public there, try ourselves.
        for address in addresses:
            if "127.0.0.1" in address and "/tcp" in address and "/p2p" in address:
                return address.replace("127.0.0.1", public_ip)

    async def get_ipfs_size(
        self, hash: str, timeout: int = 1, tries: int = 1
    ) -> Optional[int]:
        try_count = 0
        result = None
        while (result is None) and (try_count < tries):
            try_count += 1
            try:
                dag_node = await asyncio.wait_for(
                    self.storage_client.dag.get(hash), timeout=timeout
                )
                result = 0
                if isinstance(dag_node, str):
                    dag_node = json.loads(dag_node)
                if isinstance(dag_node, dict):
                    if "Data" in dag_node and isinstance(dag_node["Data"], dict):
                        # This is the common structure for UnixFS nodes after aioipfs parsing
                        if "filesize" in dag_node["Data"]:
                            result = dag_node["Data"]["filesize"]
                        elif (
                            "Tsize" in dag_node["Data"]
                        ):  # Less common, but good to check
                            result = dag_node["Data"]["Tsize"]
                        elif (
                            "Tsize" in dag_node
                        ):  # Sometimes it might be at the top level directly
                            result = dag_node["Tsize"]
                    if (
                        result == 0
                        and "Links" in dag_node
                        and isinstance(dag_node["Links"], list)
                    ):
                        total_size = 0
                        for link in dag_node["Links"]:
                            # In case it's a link list, get the Tsize property if exists
                            if "Tsize" in link and isinstance(link["Tsize"], int):
                                total_size += link["Tsize"]
                            else:
                                LOGGER.error(
                                    f"Error: CID {hash} did not return a list structure. Type: {type(link)}"
                                )
                        result = total_size

                    elif (
                        "Size" in dag_node
                    ):  # Occasionally, 'Size' might refer to total size, but often it's block
                        # size
                        result = dag_node["Size"]
                else:
                    # For raw blocks, dag_node might be bytes. block.stat is better for those.
                    # For other codecs, the structure will vary.
                    LOGGER.info(
                        f"Warning: CID {hash} did not return a dictionary structure. Type: {type(dag_node)}"
                    )

                if result == 0:
                    LOGGER.info(
                        f"INFO: CID {hash} didn't return a Size field. Executing a block stat operation"
                    )
                    block_stat = await asyncio.wait_for(
                        self.storage_client.block.stat(hash), timeout=timeout
                    )
                    result = block_stat["Size"]
            except aioipfs.APIError:
                result = None
                await asyncio.sleep(0.5)
                continue
            except asyncio.TimeoutError:
                # A timeout already consumed the full `timeout` budget, so it is
                # its own backoff: retry immediately, or give up once exhausted.
                if try_count >= tries:
                    raise FileUnavailable(
                        hash, "could not retrieve IPFS content at this time"
                    )
                continue
            except (
                concurrent.futures.CancelledError,
                aiohttp.client_exceptions.ClientConnectorError,
            ):
                try_count -= 1  # do not count as a try.
                await asyncio.sleep(0.1)

        return result

    async def get_ipfs_content(
        self, hash: str, timeout: int = 1, tries: int = 1
    ) -> Optional[bytes]:
        try_count = 0
        result = None
        while (result is None) and (try_count < tries):
            try_count += 1
            try:
                result = cast(
                    bytes,
                    await asyncio.wait_for(
                        self.storage_client.cat(hash, length=MAX_LEN), timeout=timeout
                    ),
                )
                if len(result) == MAX_LEN:
                    result = None
                    break
            except aioipfs.APIError:
                result = None
                await asyncio.sleep(0.5)
                continue
            except asyncio.TimeoutError:
                result = None
                await asyncio.sleep(0.5)
            except (
                concurrent.futures.CancelledError,
                aiohttp.client_exceptions.ClientConnectorError,
            ):
                try_count -= 1  # do not count as a try.
                await asyncio.sleep(0.1)

        if isinstance(result, str):
            result = result.encode("utf-8")

        return result

    def get_ipfs_content_iterator(self, cid: str) -> AsyncIterable[bytes]:
        params = {aioipfs.helpers.ARG_PARAM: cid}
        return _fetch_ipfs_endpoint_streamed(
            aioipfs_client=self.storage_client, endpoint="cat", params=params
        )

    def get_ipfs_directory_iterator(self, cid: str) -> AsyncIterable[bytes]:
        """Stream an IPFS directory as a tar archive using the /get endpoint."""
        params = {
            aioipfs.helpers.ARG_PARAM: cid,
            "archive": "true",
            "compress": "false",
        }
        return _fetch_ipfs_endpoint_streamed(
            aioipfs_client=self.storage_client, endpoint="get", params=params
        )

    async def get_json(self, hash, timeout=1, tries=1):
        result = await self.get_ipfs_content(hash, timeout=timeout, tries=tries)
        if result is not None and result != -1:
            try:
                result = await run_in_executor(None, json.loads, result)
            except json.decoder.JSONDecodeError:
                # try:
                #     import json as njson
                #     result = await loop.run_in_executor(None, njson.loads, result)
                # except (json.JSONDecodeError, KeyError):
                LOGGER.exception("Can't decode JSON")
                result = -1  # never retry, bogus data
        return result

    async def add_json(self, value: bytes) -> str:
        result = await self.storage_client.add_json(value)
        return result["Hash"]

    async def add_bytes(self, value: bytes, cid_version: int = 0) -> str:
        result = await self.storage_client.add_bytes(value, cid_version=cid_version)
        return result["Hash"]

    async def _pin_add(self, cid: str, timeout: int = 30):
        # ipfs pin add returns a dictionary with a progress report in terms of blocks
        # and a "Pins" key if the pinning is complete. The dictionary is empty if the daemon
        # cannot find the file on the network.
        # A value is returned every half second by the daemon. We consider that the operation
        # fails if the same value is returned timeout * 2 times.
        # This assumes that the daemon progress never goes backwards.

        tick_timeout = timeout * 2
        last_progress = None

        # Pinning is a storage operation: route through the storage client.
        async for status in self.storage_client.pin.add(cid):
            # If the Pins key appears, the file is pinned.
            if "Pins" in status:
                break

            progress = status.get("Progress")
            if progress == last_progress:
                tick_timeout -= 1
                if tick_timeout == 0:
                    if progress is None:
                        reason = "file not found"
                    else:
                        reason = "could not fetch some blocks"
                    raise FileUnavailable(cid, f"could not pin IPFS content: {reason}")
            else:
                # Reset the timeout counter if there is some measure of progress
                tick_timeout = timeout * 2

    async def pin_add(self, cid: str, timeout: int = 30, tries: int = 1):
        remaining_tries = tries

        while remaining_tries:
            try:
                await self._pin_add(cid=cid, timeout=timeout)
            except FileUnavailable:
                remaining_tries -= 1
                if not remaining_tries:
                    raise
            else:
                break

    async def dag_import(
        self,
        car_path: "pathlib.Path",
        *,
        pin_roots: bool = True,
    ) -> list[str]:
        """Stream a CAR file into kubo /api/v0/dag/import on the pinning client
        and return imported root CIDs in order.

        Raises:
            DagImportError: if the response is malformed or kubo reports a pin
                error.
            aioipfs.APIError: on a non-2xx response from kubo.
        """
        driver = self.storage_client.core.driver
        url = self.storage_client.core.url("dag/import")
        params = {
            "pin-roots": "true" if pin_roots else "false",
            "silent": "false",
            "stats": "false",
        }

        # Stream the file body as a single multipart field named "file".
        # Kubo accepts the body as application/vnd.ipld.car directly, but the
        # official client uploads it via multipart; replicate that to stay on
        # the well-trodden path.
        #
        # We use aiofiles so that reads happen on the threadpool executor
        # rather than blocking the event loop. aiohttp's payload registry
        # picks up the aiofiles handle via AsyncIterablePayload because it
        # implements __aiter__/__anext__.
        async with aiofiles.open(car_path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field(
                "file",
                f,
                filename=car_path.name,
                content_type="application/vnd.ipld.car",
            )
            async with driver.session.post(
                url, params=params, data=data, auth=driver.auth
            ) as response:
                body = await response.read()
                if response.status in aioipfs.apis.HTTP_ERROR_CODES:
                    self.storage_client.core.handle_error(response, body)

        return parse_dag_import_response(body)

    async def verify_ipns_record(self, record: bytes, name: str) -> IpnsRecordInfo:
        """
        Verify a signed IPNS record against its name via kubo and extract
        the value CID, sequence number and end-of-life.
        """
        client = self.storage_client
        # kubo's name/inspect endpoint takes the record as a file upload;
        # the aioipfs wrapper only accepts a file path.
        with tempfile.NamedTemporaryFile() as record_file:
            record_file.write(record)
            record_file.flush()
            response = await client.name.inspect(
                record=record_file.name, verify=name, dump=False
            )

        validation = response.get("Validation") or {}
        if not validation.get("Valid"):
            raise InvalidIpnsRecordError(
                validation.get("Reason") or "record failed validation"
            )

        entry = response["Entry"]
        value = entry["Value"]
        if not value.startswith("/ipfs/"):
            raise InvalidIpnsRecordError(f"unsupported IPNS value: {value}")
        cid = value.removeprefix("/ipfs/")
        if "/" in cid:
            raise InvalidIpnsRecordError(
                f"IPNS records with sub-path values are not supported: {value}"
            )

        validity = dt.datetime.fromisoformat(entry["Validity"].replace("Z", "+00:00"))
        return IpnsRecordInfo(
            value_cid=cid, sequence=int(entry["Sequence"]), validity=validity
        )

    async def resolve_ipns_record(self, name: str, timeout: int) -> bytes:
        """Fetch the current signed record for an IPNS name from the DHT."""
        client = self.storage_client
        try:
            response = await asyncio.wait_for(
                client.routing.get(f"/ipns/{name}"), timeout
            )
        except (asyncio.TimeoutError, aioipfs.APIError) as e:
            raise IpnsResolutionError(name) from e

        extra = (response or {}).get("Extra")
        if not extra:
            raise IpnsResolutionError(name)
        return base64.b64decode(extra)

    async def put_ipns_record(self, name: str, record: bytes) -> None:
        """
        Inject/republish a signed IPNS record into the DHT.

        The aioipfs routing.put wrapper passes the value as a query
        argument, which kubo rejects for binary records; post the record
        as a multipart body the way name/inspect does.
        """
        from aioipfs import multi

        client = self.storage_client
        with tempfile.NamedTemporaryFile() as record_file:
            record_file.write(record)
            record_file.flush()
            with multi.FormDataWriter() as mpwriter:
                mpwriter.append_payload(multi.bytes_payload_from_file(record_file.name))
                await client.routing.post(
                    client.routing.url("routing/put"),
                    mpwriter,
                    params={"arg": f"/ipns/{name}"},
                    outformat="json",
                )
        return None

    async def sub(self, topic: str):
        ipfs_client = self.ipfs_client

        try:
            async for mvalue in ipfs_client.pubsub.sub(topic):
                try:
                    LOGGER.debug("New message received %r" % mvalue)

                    # we should check the sender here to avoid spam
                    # and such things...
                    yield mvalue

                except Exception:
                    LOGGER.exception("Error handling message")
        except Exception:
            LOGGER.exception("Error handling IPFS subscription")

    async def pub(self, topic: str, message: Union[str, bytes]):
        # aioipfs only accepts strings
        message_str = message if isinstance(message, str) else message.decode("utf-8")

        ipfs_client = self.ipfs_client
        await ipfs_client.pubsub.pub(topic, message_str)


def _should_use_separate_pinning_client(config: Config) -> bool:
    """
    Determine if we should use a separate IPFS client for pinning operations.
    Returns True if pinning configuration is different from main IPFS configuration.
    """
    if not hasattr(config.ipfs, "pinning"):
        return False

    # Check if pinning host/port are specifically configured and different
    pinning_host = config.ipfs.pinning.host.value
    pinning_port = config.ipfs.pinning.port.value

    if pinning_host and pinning_port:
        main_host = config.ipfs.host.value
        main_port = config.ipfs.port.value
        return (pinning_host != main_host) or (pinning_port != main_port)

    return False
