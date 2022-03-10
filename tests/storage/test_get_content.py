import json

import pytest
from configmanager import Config

from aleph.config import get_defaults
from aleph.storage import ContentSource, get_hash_content, get_json, get_message_content
from aleph.exceptions import InvalidContent, ContentCurrentlyUnavailable
from aleph.types import ItemType
from aleph.web import app


@pytest.fixture
def mock_config(mocker):
    config = Config(get_defaults())
    mock_config = mocker.patch.dict(app, {"config": config})
    return mock_config


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "use_network,use_ipfs", [(False, False), (True, False), (False, True), (True, True)]
)
async def test_hash_content_from_db(
    mocker, mock_config, use_network: bool, use_ipfs: bool
):
    expected_content = b"fluctuat nec mergitur"
    mocker.patch("aleph.storage.get_value", return_value=expected_content)

    content_hash = "1234"
    content = await get_hash_content(
        content_hash, use_network=use_network, use_ipfs=use_ipfs
    )
    assert content.value == expected_content
    assert content.hash == content_hash
    assert content.source == ContentSource.DB
    assert len(content) == len(expected_content)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_ipfs", [False, True])
async def test_hash_content_from_network(mocker, mock_config, use_ipfs: bool):
    content_hash = "1234"
    expected_content = b"elementary my dear Watson"

    mocker.patch("aleph.storage.get_value", return_value=None)
    mocker.patch("aleph.storage.p2p_http_request_hash", return_value=expected_content)
    mocker.patch("aleph.storage.verify_content_hash")

    content = await get_hash_content(
        content_hash, use_network=True, use_ipfs=use_ipfs, store_value=False
    )
    assert content.value == expected_content
    assert content.hash == content_hash
    assert content.source == ContentSource.P2P
    assert len(content) == len(expected_content)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_ipfs", [False, True])
async def test_hash_content_from_network_store_value(
    mocker, mock_config, use_ipfs: bool
):
    content_hash = "1234"
    expected_content = b"mais ou est donc ornicar?"

    mocker.patch("aleph.storage.get_value", return_value=None)
    mocker.patch("aleph.storage.p2p_http_request_hash", return_value=expected_content)
    mocker.patch("aleph.storage.verify_content_hash", return_value=content_hash)
    set_value_mock = mocker.patch("aleph.storage.set_value")

    content = await get_hash_content(
        content_hash, use_network=True, use_ipfs=use_ipfs, store_value=True
    )
    assert content.value == expected_content
    assert content.hash == content_hash
    assert content.source == ContentSource.P2P
    assert len(content) == len(expected_content)

    set_value_mock.assert_called_once_with(content_hash, expected_content)


@pytest.mark.asyncio
async def test_hash_content_from_network_invalid_hash(mocker, mock_config):
    content_hash = "1234"
    expected_content = b"carpe diem"

    mocker.patch("aleph.storage.get_value", return_value=None)
    mocker.patch("aleph.storage.p2p_http_request_hash", return_value=expected_content)
    mocker.patch("aleph.storage.get_cid_version", return_value=1)
    mocker.patch("aleph.storage.add_ipfs_bytes", return_value="not-the-same-hash")

    with pytest.raises(InvalidContent):
        _content = await get_hash_content(
            content_hash, use_network=True, use_ipfs=False, store_value=False
        )


@pytest.mark.asyncio
async def test_hash_content_from_ipfs(mocker, mock_config):
    content_hash = "1234"
    expected_content = b"cave canem"

    mocker.patch("aleph.storage.get_value", return_value=None)
    mocker.patch("aleph.storage.p2p_http_request_hash", return_value=expected_content)
    mocker.patch("aleph.storage.get_cid_version", return_value=1)
    mocker.patch(
        "aleph.storage.add_ipfs_bytes", return_value="not-the-hash-you're-looking-for"
    )

    with pytest.raises(InvalidContent):
        _content = await get_hash_content(
            content_hash, use_network=True, use_ipfs=False, store_value=False
        )


@pytest.mark.asyncio
async def test_get_valid_json(mocker, mock_config):
    content_hash = "some-json-hash"
    json_content = {"1": "one", "2": "two", "3": "three"}
    json_bytes = json.dumps(json_content).encode("utf-8")
    mocker.patch("aleph.storage.get_value", return_value=json_bytes)

    content = await get_json(content_hash)
    assert content.value == json_content
    assert content.hash == content_hash
    assert content.raw_value == json_bytes


@pytest.mark.asyncio
async def test_get_invalid_json(mocker, mock_config):
    """
    Checks that retrieving non-JSON content using get_json fails.
    """

    non_json_content = b"<span>How do you like HTML?</span"
    mocker.patch("aleph.storage.get_value", return_value=non_json_content)

    with pytest.raises(InvalidContent):
        _content = await get_json("1234")


@pytest.mark.asyncio
async def test_get_inline_content(mock_config):
    content_hash = "message-hash"
    json_content = [
        {"post": "The joys of JavaScript (JK)"},
        {"post": "Does your cat plan to murder you?"},
    ]
    json_bytes = json.dumps(json_content).encode("utf-8")
    message = {
        "item_type": ItemType.Inline.value,
        "item_hash": content_hash,
        "item_content": json_bytes,
    }

    content = await get_message_content(message)
    assert content.value == json_content
    assert content.hash == content_hash
    assert content.raw_value == json_bytes


@pytest.mark.asyncio
async def test_get_inline_content_full_message():
    """
    Same test as above, with a complete message. Reuse of an older test.
    """

    msg = {
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
    content = await get_message_content(msg)
    item_content = content.value
    print(item_content)
    assert len(content.raw_value) == len(msg['item_content'])
    assert item_content['key'] == 'metrics'
    assert item_content['address'] == 'TTapAav8g3fFjxQQCjwPd4ERPnai9oya'
    assert 'memory' in item_content['content']
    assert 'cpu_cores' in item_content['content']


@pytest.mark.asyncio
async def test_get_stored_message_content(mocker, mock_config):
    content_hash = "message-hash"
    json_content = {"I": "Inter", "P": "Planetary", "F": "File", "S": "System"}
    json_bytes = json.dumps(json_content).encode("utf-8")
    mocker.patch("aleph.storage.get_value", return_value=json_bytes)

    message = {
        "item_type": ItemType.IPFS.value,
        "item_hash": content_hash,
    }

    content = await get_message_content(message)
    assert content.value == json_content
    assert content.hash == content_hash


@pytest.mark.asyncio
async def test_get_message_content_unknown_item_type(mocker, mock_config):
    """
    Checks that an unknown item type field in a message will mark it as currently unavailable.
    """

    json_content = {"No more": "inspiration"}
    json_bytes = json.dumps(json_content).encode("utf-8")
    mocker.patch("aleph.storage.get_value", return_value=json_bytes)

    message = {
        "item_type": "invalid-item-type",
        "item_hash": "message_hash",
    }

    with pytest.raises(ContentCurrentlyUnavailable):
        _content = await get_message_content(message)
