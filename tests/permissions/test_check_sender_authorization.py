import pytest

from aleph.model.messages import Message
from aleph.permissions import check_sender_authorization
from message_test_helpers import make_validated_message_from_dict


@pytest.mark.asyncio
async def test_owner_is_sender():
    message_dict = {
        "chain": "ETH",
        "item_hash": "2a5aaf71c8767bda8eb235223a3387b310af117f42fac08f02461e90aee073b0",
        "sender": "0xdeF61fAadE93a8aaE303D083Ead5BF7a25E55a23",
        "type": "STORE",
        "channel": "TEST",
        "item_content": '{"address":"0xdeF61fAadE93a8aaE303D083Ead5BF7a25E55a23","item_type":"storage","item_hash":"e916165d63c9b1d455dc415859ec3e1da5a3c6c86cc743cbedf2203fd92a2b1b","time":1652085236.777}',
        "item_type": "inline",
        "signature": "0x51383ef8823665bd8ea1150175be0c3745a36ea1f0d503ceb51e0d7ff1fd88a5290665564bf9c2315d97884e7448efdb8d4b4f8293b47a641c2ff43f21b6c5b61c",
        "time": 1652085236.777,
    }

    message = make_validated_message_from_dict(message_dict, message_dict["item_content"])

    is_authorized = await check_sender_authorization(message)
    assert is_authorized


@pytest.mark.asyncio
async def test_store_unauthorized(mocker):
    mocker.patch("aleph.permissions.get_computed_address_aggregates", return_value={})

    message_dict = {
        "chain": "ETH",
        "channel": "TEST",
        "item_content": '{"address":"VM on executor","time":1651050219.3481126,"content":{"date":"2022-04-27T09:03:38.361081","test":true,"answer":42,"something":"interesting"},"type":"test"}',
        "item_hash": "498a10255877a74609654b673af4f8f29eb8ef1aa5d6265d9a6bf9e342d352db",
        "item_type": "inline",
        "sender": "0x8b5C865d6ff6Dd5C5c402C8D918F7edd189C74D4",
        "signature": "0xad5101992e1bf71bd292883bdbcf4aee761c9c4d8020a9eabfeec3367ed7c85e25fa73fccf253b343ff9f014f11aaaf2a25ae89dbaebf6f11e1523ea695c0c231b",
        "time": 1651050219.3488848,
        "type": "POST",
    }

    message = make_validated_message_from_dict(message_dict, message_dict["item_content"])

    is_authorized = await check_sender_authorization(message)
    assert not is_authorized


AUTHORIZED_MESSAGE = {
    "chain": "ETH",
    "channel": "TEST",
    "item_content": '{"address":"0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58","time":1651050219.3481126,"content":{"date":"2022-04-27T09:03:38.361081","test":true,"answer":42,"something":"interesting"},"type":"test"}',
    "item_hash": "1d8c28dac67725dd9d0ed218127d5ef7870443c803cd35598bb6cbb03ec76383",
    "item_type": "inline",
    "sender": "0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E",
    "time": 1651050219.3488848,
    "type": "POST",
    "signature": "fake signature, not checked here<",
}


@pytest.mark.asyncio
async def test_authorized(mocker):
    mocker.patch(
        "aleph.permissions.get_computed_address_aggregates",
        return_value={
            "0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58": {
                "security": {
                    "authorizations": [
                        {"address": "0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E"}
                    ]
                }
            }
        },
    )

    message = make_validated_message_from_dict(AUTHORIZED_MESSAGE, AUTHORIZED_MESSAGE["item_content"])

    is_authorized = await check_sender_authorization(message)
    assert is_authorized


@pytest.mark.asyncio
async def test_authorized_with_db(test_db):
    security_message = {
        "chain": "ETH",
        "item_hash": "f58e4f46268bd665d90cb0a65cce0754394c9f3f27a9b9d9228a03c59ea61c56",
        "sender": "0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58",
        "type": "AGGREGATE",
        "channel": "security",
        "confirmations": [
            {
                "chain": "ETH",
                "height": 13753606,
                "hash": "0xcadd015d263f0d713493d7ef489df70cdac70d4873382b5cb0dc9bc5ef348b56",
            }
        ],
        "confirmed": True,
        "content": {
            "address": "0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58",
            "key": "security",
            "content": {
                "authorizations": [
                    {"address": "0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E"}
                ]
            },
            "time": 1638808268.426,
        },
        "item_content": '{"address":"0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58","key":"security","content":{"authorizations":[{"address":"0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E"}]},"time":1638808268.426}',
        "item_type": "inline",
        "signature": "0xd76d59602ae23b0178664705fd5a03cc8109cc4753593dad4ebbd7dec8e9396119a5f96449fb7aee85313b79b05c83193165f13785f634b506a98826f8c076e51c",
        "size": 183,
        "time": 1638811994.011,
    }

    await Message.collection.insert_one(security_message)

    message = make_validated_message_from_dict(AUTHORIZED_MESSAGE, AUTHORIZED_MESSAGE["item_content"])

    is_authorized = await check_sender_authorization(message)
    assert is_authorized
