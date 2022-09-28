from typing import Mapping

import pytest
from message_test_helpers import make_validated_message_from_dict

from aleph.handlers.forget import ForgetMessageHandler, TargetMessageInfo


@pytest.fixture
def forget_handler(mocker) -> ForgetMessageHandler:
    return ForgetMessageHandler(storage_service=mocker.AsyncMock())


@pytest.mark.asyncio
async def test_forget_inline_message(mocker, forget_handler: ForgetMessageHandler):
    target_message = {
        "chain": "ETH",
        "channel": "TEST",
        "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        "type": "POST",
        "time": 1652786281.9810653,
        "item_type": "inline",
        "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652786281.980474,"content":{"body":"This message will be destroyed"},"type":"test"}',
        "item_hash": "fc1e7b1edc2348eb78303fb1342e31e5ad3820249629032d37f8223e754a5f8e",
        "signature": "0xdd8f7061d3c8e7019110b6dc0697c71ae8da5295e26f1d20c265bcb78fc616a05d3927f72888a459c048a297ff17c748ad3803e5f95bf000e3e4c0feba350e101c",
        "content": {
            "address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            "time": 1652786281.980474,
            "content": {"body": "This message will be destroyed"},
            "type": "test",
        },
    }
    forget_message = {
        "chain": "ETH",
        "channel": "TEST",
        "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        "type": "FORGET",
        "time": 1652786534.1139255,
        "item_type": "inline",
        "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652786534.1138077,"hashes":["fc1e7b1edc2348eb78303fb1342e31e5ad3820249629032d37f8223e754a5f8e"]}',
        "item_hash": "431a0d2f79ecfa859949d2a09f67068ce7ebd4eb777d179ad958be6c79abc66b",
        "signature": "0x409cdef65af51d6a508a1fdc56c0baa6d1abac7f539ab5f290e3245c522a4c766b930c4196d9f5d8c8c94a4d36c4b65bf04a2773f058f03803b9b0bca2fd85a51b",
        "content": {
            "address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            "time": 1652786534.1138077,
            "hashes": [
                "fc1e7b1edc2348eb78303fb1342e31e5ad3820249629032d37f8223e754a5f8e"
            ],
        },
    }

    forget_message = make_validated_message_from_dict(forget_message)
    forget_handler.garbage_collect = garbage_collect_mock = mocker.AsyncMock()  # type: ignore
    message_mock = mocker.patch("aleph.handlers.forget.Message")
    message_mock.collection.update_many = mocker.AsyncMock()

    target_info = TargetMessageInfo.from_db_object(target_message)
    await forget_handler.forget_if_allowed(target_info, forget_message)

    message_mock.collection.update_many.assert_called_once_with(
        filter={"item_hash": target_message["item_hash"]},
        update={
            "$set": {
                "content": None,
                "item_content": None,
                "forgotten_by": [forget_message.item_hash],
            }
        },
    )
    assert not garbage_collect_mock.called


@pytest.mark.asyncio
async def test_forget_store_message(mocker, forget_handler: ForgetMessageHandler):
    target_message: Mapping = {
        "chain": "ETH",
        "channel": "TEST",
        "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        "type": "STORE",
        "time": 1652794362.573859,
        "item_type": "inline",
        "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652794362.5736332,"item_type":"storage","item_hash":"5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21","mime_type":"text/plain"}',
        "item_hash": "f6fc4884e3ec3624bd3f60a3c37abf83a130777086061b1a373e659f2bab4d06",
        "signature": "0x7b87c29388a7a452353f9cae8718b66158fb5bdc93f032964226745ee04919092550791b93f79e5ee1981f2d9d6e5ac0cae0d28b68bb63fe0fcbd79015a6f3ea1b",
        "content": {
            "address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            "time": 1652794362.5736332,
            "item_type": "storage",
            "item_hash": "5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21",
            "mime_type": "text/plain",
        },
    }

    forget_message = {
        "chain": "ETH",
        "channel": "TEST",
        "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        "type": "FORGET",
        "time": 1652794384.3102906,
        "item_type": "inline",
        "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652794384.3101473,"hashes":["f6fc4884e3ec3624bd3f60a3c37abf83a130777086061b1a373e659f2bab4d06"]}',
        "item_hash": "5e40c8e2197e0678b5fba9cb1679e3a80fa6aeaa1a440d94f059525295fa32d3",
        "signature": "0xc342e671be10894bf707b86c3f7538cdb7e4bb5760e234f8d07f8b3dfde015492337bd8756f169e37ac691b74c765415e96b6e1813238912e10ea54cc003887d1b",
    }

    forget_message = make_validated_message_from_dict(forget_message)
    forget_handler.garbage_collect = garbage_collect_mock = mocker.AsyncMock()  # type: ignore
    message_mock = mocker.patch("aleph.handlers.forget.Message")
    message_mock.collection.update_many = mocker.AsyncMock()

    target_info = TargetMessageInfo.from_db_object(target_message)
    await forget_handler.forget_if_allowed(target_info, forget_message)

    message_mock.collection.update_many.assert_called_once_with(
        filter={"item_hash": target_message["item_hash"]},
        update={
            "$set": {
                "content": None,
                "item_content": None,
                "forgotten_by": [forget_message.item_hash],
            }
        },
    )
    assert garbage_collect_mock.called_once_with(
        storage_hash=target_message["content"]["item_hash"]
    )


@pytest.mark.asyncio
async def test_forget_forget_message(mocker, forget_handler: ForgetMessageHandler):
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

    forget_message = make_validated_message_from_dict(forget_message)
    forget_handler.garbage_collect = garbage_collect_mock = mocker.AsyncMock()  # type: ignore

    message_mock = mocker.patch("aleph.handlers.forget.Message")
    message_mock.collection.update_many = mocker.AsyncMock()

    target_info = TargetMessageInfo.from_db_object(target_message)
    await forget_handler.forget_if_allowed(target_info, forget_message)

    assert not message_mock.collection.update_many.called
    assert not garbage_collect_mock.called
