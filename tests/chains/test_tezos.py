import pytest

from aleph.chains import tezos  # TODO: this import is currently necessary because of circular dependencies
from aleph.network import check_message


@pytest.mark.asyncio
async def test_tezos_verify_signature():
    message_dict = {
        "chain": "TEZOS",
        "channel": "TEST",
        "sender": "tz2M3NQJ982QV1YwvGL77drZCY55tfBzWm3x",
        "type": "POST",
        "time": 1657534863.1375434,
        "item_content": '{"address":"tz2M3NQJ982QV1YwvGL77drZCY55tfBzWm3x","time":1657534863.1371658,"content":{"status":"testing"},"type":"test"}',
        "item_hash": "75bce3f296c6479242c740a051c2ddef4184f39386a883ceb33bf1f36f45ad09",
        "signature": '{"publicKey": "sppk7cKmJSNo8LxB6R3eUGGRg3Lt7xn6K4wRNNxjSLxeB5zPZhvkQ6k", "signature": "spsig1Z3B4PduY14W2FjbHPuaYG8CRSFzJiZeNDPhdivyiDGqyAG1ZRKtubTmq7zfKywekzUBaAdop6mxoodBfq8Yi9RqroECk5"}',
        "content": {
            "address": "tz2M3NQJ982QV1YwvGL77drZCY55tfBzWm3x",
            "time": 1657534863.1371658,
            "content": {"status": "testing"},
            "type": "test",
        },
    }

    _ = await check_message(message_dict)
