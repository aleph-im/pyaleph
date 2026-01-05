import json
from typing import AsyncIterable, Dict, Optional

import pytest

from aleph.exceptions import InvalidContent
from aleph.schemas.message_content import ContentSource
from aleph.schemas.pending_messages import parse_message
from aleph.services.ipfs import IpfsService
from aleph.services.storage.engine import StorageEngine
from aleph.storage import StorageService


class MockStorageEngine(StorageEngine):
    def __init__(self, files: Dict[str, bytes]):
        self.files = files

    async def read(self, filename: str) -> Optional[bytes]:
        try:
            return self.files[filename]
        except KeyError:
            return None

    async def read_iterator(
        self, filename: str, chunk_size: int = 1024 * 1024
    ) -> Optional[AsyncIterable[bytes]]:
        content = await self.read(filename)
        if content is None:
            return None

        async def _read_iterator():
            for i in range(0, len(content), chunk_size):
                yield content[i : i + chunk_size]

        return _read_iterator()

    async def write(self, filename: str, content: bytes):
        self.files[filename] = content

    async def delete(self, filename: str):
        del self.files[filename]

    async def exists(self, filename: str) -> bool:
        return filename in self.files


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "use_network,use_ipfs", [(False, False), (True, False), (False, True), (True, True)]
)
async def test_hash_content_from_db(mocker, use_network: bool, use_ipfs: bool):
    storage_manager = StorageService(
        MockStorageEngine(files={"1234": b"fluctuat nec mergitur"}),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    expected_content = b"fluctuat nec mergitur"

    content_hash = "1234"
    content = await storage_manager.get_hash_content(
        content_hash, use_network=use_network, use_ipfs=use_ipfs
    )
    assert content.value == expected_content
    assert content.hash == content_hash
    assert content.source == ContentSource.DB
    assert len(content) == len(expected_content)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_ipfs", [False, True])
async def test_hash_content_from_network(mocker, use_ipfs: bool):
    content_hash = "1234"
    expected_content = b"elementary my dear Watson"

    mocker.patch("aleph.storage.p2p_http_request_hash", return_value=expected_content)
    mocker.patch.object(StorageService, "_verify_content_hash")

    # No files in the local storage
    storage_manager = StorageService(
        MockStorageEngine(files={}),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    content = await storage_manager.get_hash_content(
        content_hash, use_network=True, use_ipfs=use_ipfs, store_value=False
    )
    assert content.value == expected_content
    assert content.hash == content_hash
    assert content.source == ContentSource.P2P
    assert len(content) == len(expected_content)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_ipfs", [False, True])
async def test_hash_content_from_network_store_value(mocker, use_ipfs: bool):
    content_hash = "1234"
    expected_content = b"mais ou est donc ornicar?"

    mocker.patch("aleph.storage.p2p_http_request_hash", return_value=expected_content)
    mocker.patch.object(
        StorageService, "_verify_content_hash", return_value=content_hash
    )

    storage_manager = StorageService(
        MockStorageEngine(files={}),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    content = await storage_manager.get_hash_content(
        content_hash, use_network=True, use_ipfs=use_ipfs, store_value=True
    )
    assert content.value == expected_content
    assert content.hash == content_hash
    assert content.source == ContentSource.P2P
    assert len(content) == len(expected_content)

    content = await storage_manager.storage_engine.read(content_hash)
    assert content == expected_content


@pytest.mark.asyncio
async def test_hash_content_from_network_invalid_hash(mocker):
    content_hash = "1234"
    expected_content = b"carpe diem"

    mocker.patch("aleph.storage.p2p_http_request_hash", return_value=expected_content)
    mocker.patch("aleph.storage.get_cid_version", return_value=1)

    ipfs_client = mocker.AsyncMock()
    ipfs_client.add_bytes = mocker.AsyncMock(return_value={"Hash": "not-the-same-hash"})
    ipfs_service = IpfsService(ipfs_client=ipfs_client)

    storage_manager = StorageService(
        MockStorageEngine(files={}),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    with pytest.raises(InvalidContent):
        _content = await storage_manager.get_hash_content(
            content_hash, use_network=True, use_ipfs=False, store_value=False
        )


@pytest.mark.asyncio
async def test_hash_content_from_ipfs(mocker):
    content_hash = "1234"
    expected_content = b"cave canem"

    mocker.patch("aleph.storage.p2p_http_request_hash", return_value=expected_content)
    mocker.patch("aleph.storage.get_cid_version", return_value=1)

    ipfs_client = mocker.AsyncMock()
    ipfs_client.add_bytes = mocker.AsyncMock(
        return_value={"Hash": "not-the-hash-you're-looking-for"}
    )
    ipfs_service = IpfsService(ipfs_client=ipfs_client)

    storage_manager = StorageService(
        MockStorageEngine(files={}),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    with pytest.raises(InvalidContent):
        _content = await storage_manager.get_hash_content(
            content_hash, use_network=True, use_ipfs=False, store_value=False
        )


@pytest.mark.asyncio
async def test_get_valid_json(mocker):
    content_hash = "some-json-hash"
    json_content = {"1": "one", "2": "two", "3": "three"}
    json_bytes = json.dumps(json_content).encode("utf-8")

    storage_manager = StorageService(
        MockStorageEngine(files={content_hash: json_bytes}),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    content = await storage_manager.get_json(content_hash)
    assert content.value == json_content
    assert content.hash == content_hash
    assert content.raw_value == json_bytes


@pytest.mark.asyncio
async def test_get_invalid_json(mocker):
    """
    Checks that retrieving non-JSON content using get_json fails.
    """

    non_json_content = b"<span>How do you like HTML?</span"
    storage_manager = StorageService(
        MockStorageEngine(files={"1234": non_json_content}),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    with pytest.raises(InvalidContent):
        _content = await storage_manager.get_json("1234")


@pytest.mark.asyncio
async def test_get_inline_content_full_message(mocker):
    """
    Get inline content from a message. Reuses an older test/fixture.
    """

    message_dict = {
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTapAav8g3fFjxQQCjwPd4ERPnai9oya",
        "type": "AGGREGATE",
        "time": 1564581054.0532622,
        "item_content": '{"key":"metrics","address":"TTapAav8g3fFjxQQCjwPd4ERPnai9oya","content":{"memory":{"total":12578275328,"available":5726081024,"percent":54.5,"used":6503415808,"free":238661632,"active":8694841344,"inactive":2322239488,"buffers":846553088,"cached":4989644800,"shared":172527616,"slab":948609024},"swap":{"total":7787769856,"free":7787495424,"used":274432,"percent":0.0,"swapped_in":0,"swapped_out":16384},"cpu":{"user":9.0,"nice":0.0,"system":3.1,"idle":85.4,"iowait":0.0,"irq":0.0,"softirq":2.5,"steal":0.0,"guest":0.0,"guest_nice":0.0},"cpu_cores":[{"user":8.9,"nice":0.0,"system":2.4,"idle":82.2,"iowait":0.0,"irq":0.0,"softirq":6.4,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":9.6,"nice":0.0,"system":2.9,"idle":84.6,"iowait":0.0,"irq":0.0,"softirq":2.9,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":7.2,"nice":0.0,"system":3.0,"idle":86.8,"iowait":0.0,"irq":0.0,"softirq":3.0,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":11.4,"nice":0.0,"system":3.0,"idle":84.8,"iowait":0.1,"irq":0.0,"softirq":0.7,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":9.3,"nice":0.0,"system":3.3,"idle":87.0,"iowait":0.1,"irq":0.0,"softirq":0.3,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":5.5,"nice":0.0,"system":4.4,"idle":89.9,"iowait":0.0,"irq":0.0,"softirq":0.1,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":8.7,"nice":0.0,"system":3.3,"idle":87.9,"iowait":0.0,"irq":0.0,"softirq":0.1,"steal":0.0,"guest":0.0,"guest_nice":0.0},{"user":11.4,"nice":0.0,"system":2.3,"idle":80.3,"iowait":0.0,"irq":0.0,"softirq":6.1,"steal":0.0,"guest":0.0,"guest_nice":0.0}]},"time":1564581054.0358574}',
        "item_hash": "84afd8484912d3fa11a402e480d17e949fbf600fcdedd69674253be0320fa62c",
        "signature": "21027c108022f992f090bbe5c78ca8822f5b7adceb705ae2cd5318543d7bcdd2a74700473045022100b59f7df5333d57080a93be53b9af74e66a284170ec493455e675eb2539ac21db022077ffc66fe8dde7707038344496a85266bf42af1240017d4e1fa0d7068c588ca7",
        "item_type": "inline",
    }

    storage_manager = StorageService(
        MockStorageEngine(files={}),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    message = parse_message(message_dict)
    content = await storage_manager.get_message_content(message)
    item_content = content.value

    assert len(content.raw_value) == len(message.item_content)
    assert item_content["key"] == "metrics"
    assert item_content["address"] == "TTapAav8g3fFjxQQCjwPd4ERPnai9oya"
    assert "memory" in item_content["content"]
    assert "cpu_cores" in item_content["content"]


@pytest.mark.asyncio
async def test_get_stored_message_content(mocker):
    message_dict = {
        "chain": "ETH",
        "channel": "TEST",
        "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        "type": "POST",
        "item_type": "storage",
        "item_hash": "315f7313eb97d2c8299e3ee9c19d81f226c44ccf81c387c9fb25c54fced245f5",
        "item_content": None,
        "signature": "unsigned fixture, deal with it",
        "time": 1652805847.190618,
    }
    json_content = {"I": "Inter", "P": "Planetary", "F": "File", "S": "System"}
    json_bytes = json.dumps(json_content).encode("utf-8")

    message = parse_message(message_dict)
    storage_manager = StorageService(
        MockStorageEngine(files={message.item_hash: json_bytes}),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )

    content = await storage_manager.get_message_content(message)
    assert content.value == json_content
    assert content.hash == message.item_hash
