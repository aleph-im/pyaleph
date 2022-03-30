import json

from aleph.chains.on_chain_models import (
    OnChainProtocol,
    OnChainAlephProtocolData,
    OnChainAlephOffChainProtocolData,
    parse_on_chain_data,
)


def test_on_chain_data_deserialize_aleph_json():
    json_data = {
        "protocol": "aleph",
        "version": 1,
        "content": [
            {
                "chain": "ETH",
                "item_hash": "e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5",
                "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
                "type": "POST",
                "channel": "TEST",
                "confirmed": True,
                "content": None,
                "item_content": '{"body": "This message will be destroyed"}',
                "item_type": "inline",
                "signature": "0x44376d814eb42e60592d4d7eb7d6b7a13954cd829ee394a88d1e8826f606841c0473abb94984349694a3ab61164266346574cf5cdcde9d1267793c06a38468fc1b",
                "size": 154,
                "time": 1639058229.327,
            }
        ],
    }
    json_str = json.dumps(json_data)

    on_chain_data = parse_on_chain_data(json_str)

    assert isinstance(on_chain_data, OnChainAlephProtocolData)

    assert on_chain_data.protocol == OnChainProtocol.ALEPH
    assert on_chain_data.version == 1
    assert on_chain_data.content == json_data["content"]


def test_on_chain_data_deserialize_aleph_offchain_json():
    item_hash = "b0b98c81058e2228090b4c620cf2eb1b7042565fb509a1aaf2ebf6e9c029eafc"
    json_data = {"protocol": "aleph-offchain", "version": 1, "content": item_hash}
    json_str = json.dumps(json_data)

    on_chain_data = parse_on_chain_data(json_str)

    assert isinstance(on_chain_data, OnChainAlephOffChainProtocolData)

    assert on_chain_data.protocol == OnChainProtocol.ALEPH_OFF_CHAIN
    assert on_chain_data.version == 1
    assert on_chain_data.content == json_data["content"]


def test_on_chain_data_deserialize_serialized_string():
    item_hash = "b0b98c81058e2228090b4c620cf2eb1b7042565fb509a1aaf2ebf6e9c029eafc"
    data_str = f"21{item_hash}"

    on_chain_data = parse_on_chain_data(data_str)

    assert isinstance(on_chain_data, OnChainAlephOffChainProtocolData)

    assert on_chain_data.protocol == OnChainProtocol.ALEPH_OFF_CHAIN
    assert on_chain_data.version == 1
    assert on_chain_data.content == item_hash
