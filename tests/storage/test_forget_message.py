import pytest
from aleph_message.models import ForgetMessage
from aleph.handlers.forget import forget_if_allowed


@pytest.mark.asyncio
async def test_forget_inline_message(mocker):
    target_message = {
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
    forget_message = {
        "chain": "ETH",
        "item_hash": "884dd713e94fa0350239b67e65eecaa54361df8af0e3f6d0e42e0f8de059e15a",
        "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        "type": "FORGET",
        "channel": "TEST",
        "content": {
            "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "time": 1639058312.376,
            "hashes": [
                "e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"
            ],
            "reason": "None",
        },
        "item_content": '{"address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1639058312.376,"hashes":["e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"],"reason":"None"}',
        "item_type": "inline",
        "signature": "0x7dc7a45aab12d78367c085799d06ef2e98fce31f76ca06975ce570fe4d92008f66f307bf68ed3ca450d04d4e779776ca13a1e7851cb48915bd390389ae4afd1b1c",
        "size": 172,
        "time": 1639058312.376,
    }

    forget_message = ForgetMessage(**forget_message)
    garbage_collect_mock = mocker.patch("aleph.handlers.forget.garbage_collect")
    message_mock = mocker.patch("aleph.handlers.forget.Message")
    await forget_if_allowed(target_message, forget_message)

    assert message_mock.collection.update_many.called_once_with(
        filter={"item_hash": target_message["item_hash"]},
        update={
            "content": None,
            "item_content": None,
            "forgotten_by": [forget_message.item_hash],
        },
    )
    assert not garbage_collect_mock.called


@pytest.mark.asyncio
async def test_forget_store_message(mocker):
    target_message = {
        "chain": "ETH",
        "item_hash": "fcb0a45eb599305d5b8bd9ba1983317f73973befb5fb09435c5f35b00c712ae5",
        "sender": "0xEbf324C08f9b196e7dab038333c4351cCec0E23D",
        "type": "STORE",
        "channel": "ANIMA_TESTNET",
        "confirmed": False,
        "content": {
            "address": "0xEbf324C08f9b196e7dab038333c4351cCec0E23D",
            "time": 1646123806,
            "item_type": "storage",
            "item_hash": "244269f16ed6e5e597cd3f3781dd6a406fc46aba43cba46652bfa4aa5b889145",
            "size": 220916,
            "content_type": "file",
        },
        "item_content": '{"address":"0xEbf324C08f9b196e7dab038333c4351cCec0E23D","time":1646123806,"item_type":"storage","item_hash":"244269f16ed6e5e597cd3f3781dd6a406fc46aba43cba46652bfa4aa5b889145","size":0,"content_type":""}',
        "item_type": "inline",
        "signature": "0xb50a1f89c2d9a0379ef30b7400416d8c602bb31006ac86aa5263d4f102f9b24f0d32293a987c26d366624850f4befc62093f283d3f0f3239d77815b3cc6833a400",
        "size": 202,
        "time": 1646123806,
    }

    forget_message = {
        "chain": "ETH",
        "item_hash": "884dd713e94fa0350239b67e65eecaa54361df8af0e3f6d0e42e0f8de059e15a",
        "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        "type": "FORGET",
        "channel": "TEST",
        "content": {
            "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "time": 1639058312.376,
            "hashes": [
                "fcb0a45eb599305d5b8bd9ba1983317f73973befb5fb09435c5f35b00c712ae5"
            ],
            "reason": "None",
        },
        "item_content": '{"address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1639058312.376,"hashes":["e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"],"reason":"None"}',
        "item_type": "inline",
        "signature": "0x7dc7a45aab12d78367c085799d06ef2e98fce31f76ca06975ce570fe4d92008f66f307bf68ed3ca450d04d4e779776ca13a1e7851cb48915bd390389ae4afd1b1c",
        "size": 172,
        "time": 1639058312.376,
    }

    forget_message = ForgetMessage(**forget_message)
    garbage_collect_mock = mocker.patch("aleph.handlers.forget.garbage_collect")
    message_mock = mocker.patch("aleph.handlers.forget.Message")
    await forget_if_allowed(target_message, forget_message)

    assert message_mock.collection.update_many.called_once_with(
        filter={"item_hash": target_message["item_hash"]},
        update={
            "content": None,
            "item_content": None,
            "forgotten_by": [forget_message.item_hash],
        },
    )
    assert garbage_collect_mock.called_once_with(
        storage_hash=target_message["content"]["item_hash"]
    )


@pytest.mark.asyncio
async def test_forget_forget_message(mocker):
    target_message = {
        "chain": "ETH",
        "item_hash": "7f849fa61c6b9cc8e8bd0a2c86abe49fcf1a77ebd05f7ce5a2cc6dfe6a69fddf",
        "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        "type": "FORGET",
        "channel": "TEST",
        "confirmed": False,
        "content": {
            "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "time": 1645794065.439,
            "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
            "reason": "None",
        },
        "item_content": '{"address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1645794065.439,"hashes":["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],"reason":"None"}',
        "item_type": "inline",
        "signature": "0xabfa661aab1a9f58955940ea213387de4773f8b1f244c2236cd4ac5ba7bf2ba902e17074bc4b289ba200807bb40951f4249668b055dc15af145b8842ecfad0601c",
        "size": 154,
        "time": 1645794065.439,
    }

    forget_message = {
        "chain": "ETH",
        "item_hash": "884dd713e94fa0350239b67e65eecaa54361df8af0e3f6d0e42e0f8de059e15a",
        "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        "type": "FORGET",
        "channel": "TEST",
        "content": {
            "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "time": 1639058312.376,
            "hashes": [
                "7f849fa61c6b9cc8e8bd0a2c86abe49fcf1a77ebd05f7ce5a2cc6dfe6a69fddf"
            ],
            "reason": "None",
        },
        "item_content": '{"address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1639058312.376,"hashes":["e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"],"reason":"None"}',
        "item_type": "inline",
        "signature": "0x7dc7a45aab12d78367c085799d06ef2e98fce31f76ca06975ce570fe4d92008f66f307bf68ed3ca450d04d4e779776ca13a1e7851cb48915bd390389ae4afd1b1c",
        "size": 172,
        "time": 1639058312.376,
    }

    forget_message = ForgetMessage(**forget_message)
    garbage_collect_mock = mocker.patch("aleph.handlers.forget.garbage_collect")
    message_mock = mocker.patch("aleph.handlers.forget.Message")
    await forget_if_allowed(target_message, forget_message)

    assert not message_mock.collection.update_many.called
    assert not garbage_collect_mock.called
