import datetime as dt
from typing import Any, Mapping

import pytest
from message_test_helpers import make_validated_message_from_dict

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.models import AggregateDb, AggregateElementDb, PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.permissions import check_sender_authorization
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import PermissionDenied


@pytest.mark.asyncio
async def test_owner_is_sender(mocker):
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

    message = make_validated_message_from_dict(
        message_dict, message_dict["item_content"]
    )

    is_authorized = await check_sender_authorization(
        session=mocker.MagicMock(), message=message
    )
    assert is_authorized


@pytest.mark.asyncio
async def test_store_unauthorized(mocker):
    mocker.patch("aleph.permissions.get_aggregate_by_key", return_value=None)

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

    message = make_validated_message_from_dict(
        message_dict, message_dict["item_content"]
    )

    is_authorized = await check_sender_authorization(
        session=mocker.MagicMock(), message=message
    )
    assert not is_authorized


AUTHORIZED_MESSAGE: Mapping[str, Any] = {
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
        "aleph.permissions.get_aggregate_by_key",
        return_value=AggregateDb(
            owner="0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58",
            key="security",
            content={
                "authorizations": [
                    {"address": "0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E"}
                ]
            },
            creation_datetime=dt.datetime(2022, 1, 1),
            last_revision_hash="1234",
        ),
    )

    message = make_validated_message_from_dict(
        AUTHORIZED_MESSAGE, AUTHORIZED_MESSAGE["item_content"]
    )

    is_authorized = await check_sender_authorization(
        session=mocker.MagicMock(), message=message
    )
    assert is_authorized


@pytest.mark.asyncio
async def test_authorized_with_db(session_factory: DbSessionFactory):
    aggregate_content = {
        "authorizations": [{"address": "0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E"}]
    }
    owner = "0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58"
    aggregate_datetime = timestamp_to_datetime(1638811994.011)
    aggregate = AggregateDb(
        key="security",
        owner=owner,
        content=aggregate_content,
        creation_datetime=aggregate_datetime,
        last_revision=AggregateElementDb(
            item_hash="f58e4f46268bd665d90cb0a65cce0754394c9f3f27a9b9d9228a03c59ea61c56",
            key="security",
            owner=owner,
            content=aggregate_content,
            creation_datetime=aggregate_datetime,
        ),
        dirty=False,
    )
    message = make_validated_message_from_dict(
        AUTHORIZED_MESSAGE, AUTHORIZED_MESSAGE["item_content"]
    )

    with session_factory() as session:
        session.add(aggregate)
        session.commit()

        is_authorized = await check_sender_authorization(
            session=session, message=message
        )
        assert is_authorized


@pytest.mark.asyncio
async def test_message_processing_should_fail_on_permission(
    mocker, session_factory: DbSessionFactory, mock_config
):
    """
    Test that reproduces the permission bug at the message processing level.
    An attacker can send a message with victim's address, and it should be rejected
    during message processing, but currently it's accepted due to the bug.
    """

    # Mock the storage and signature verification to focus on permission testing
    storage_service = mocker.Mock(spec=StorageService)
    signature_verifier = mocker.Mock(spec=SignatureVerifier)

    message_handler = MessageHandler(
        signature_verifier=signature_verifier,
        storage_service=storage_service,
        config=mock_config,
    )

    # Mock successful signature verification and content fetching
    signature_verifier.verify_signature = mocker.AsyncMock()
    storage_service.get_message_content = mocker.AsyncMock(
        return_value=mocker.Mock(
            value={
                "address": "0xVictimAccount123456789012345678901234567890",  # Victim's address
                "time": 1651050219.3481126,
                "content": {"test": True},
                "type": "test",
            },
            raw_value=b'{"address":"0xVictimAccount123456789012345678901234567890","time":1651050219.3481126,"content":{"test":true},"type":"test"}',
        )
    )

    # Mock that there's no authorization aggregate for the victim
    mocker.patch("aleph.permissions.get_aggregate_by_key", return_value=None)

    # Create a malicious pending message where sender != address in content
    malicious_message = PendingMessageDb(
        item_hash="a1b2c3d4e5f6789012345678901234567890123456789012345678901234abcd",  # Valid 64-char hash
        type="POST",
        chain="ETH",
        sender="0xAttackerAccount123456789012345678901234567890",  # Attacker's address
        signature="fake_attacker_signature",
        item_type="inline",
        channel=Channel("TEST"),
        reception_time=dt.datetime.now(),
        check_message=True,
        fetched=True,
        tx_hash=None,
    )

    with session_factory() as session:
        # BUG: This should raise PermissionDenied but currently it doesn't
        # The message gets processed successfully when it should be rejected
        try:
            await message_handler.process(
                session=session, pending_message=malicious_message
            )
            # If we reach this point, the bug exists - the message was processed when it should have been rejected
            pytest.fail(
                "BUG REPRODUCED: Message with mismatched sender/address was processed successfully when it should have been rejected with PermissionDenied"
            )
        except PermissionDenied:
            # This is the expected behavior - the message should be rejected
            pass
