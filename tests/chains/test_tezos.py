import pytest

from aleph.chains import (
    tezos,
)  # TODO: this import is currently necessary because of circular dependencies
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


@pytest.mark.asyncio
async def test_tezos_verify_signature_ed25519():
    message_dict = {
        "chain": "TEZOS",
        "sender": "tz1SmGHzna3YhKropa3WudVq72jhTPDBn4r5",
        "type": "POST",
        "channel": "ALEPH-TEST",
        "signature": '{"signature":"siggLSTX5i9ZZJHb6vUoi5gNxWEjEcBD62Jjs8JdFgDND3uc9xb5YC9bUFLpBAoudhdTRNfmV7GTnJzoWUm9y1cDh7T6KX59","publicKey":"edpkvUuhtQDPA9KfC3BY7ydh89hT34KTANMfX7L22BUrA9aGWg6QxF"}',
        "time": 1661451074.86,
        "item_type": "inline",
        "item_content": '{"type":"custom_type","address":"tz1SmGHzna3YhKropa3WudVq72jhTPDBn4r5","content":{"body":"Hello World TEZOS"},"time":1661451074.86}',
        "item_hash": "41de1a7766c7e5fad54772470eefde63b6bef8683c4159d9179d74955009deb4",
    }

    _ = await check_message(message_dict)
