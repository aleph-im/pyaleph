import datetime as dt

import pytest
from aleph_message.models import ItemHash
from configmanager import Config

from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.db.models import PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.storage import StorageService
from aleph.types.db_session import AsyncDbSessionFactory
from aleph.types.message_processing_result import ProcessedMessage, RejectedMessage
from aleph.types.message_status import ErrorCode

from .load_fixtures import load_fixture_message_list


@pytest.mark.asyncio
async def test_duplicated_forgotten_message(
    mocker,
    mock_config: Config,
    session_factory: AsyncDbSessionFactory,
    test_storage_service: StorageService,
):
    signature_verifier = mocker.AsyncMock()

    messages = load_fixture_message_list("test-data-forgotten-messages.json")

    m1 = PendingMessageDb.from_message_dict(
        messages[0], fetched=True, reception_time=dt.datetime(2025, 1, 1)
    )
    m2 = PendingMessageDb.from_message_dict(
        messages[1], fetched=True, reception_time=dt.datetime(2025, 1, 2)
    )
    m3 = PendingMessageDb.from_message_dict(
        messages[2], fetched=True, reception_time=dt.datetime(2025, 1, 3)
    )
    post_hash = m1.item_hash

    message_handler = MessageHandler(
        signature_verifier=signature_verifier,
        storage_service=test_storage_service,
        config=mock_config,
    )

    async with session_factory() as session:
        # 1) process post message
        test1 = await message_handler.process(
            session=session,
            pending_message=m1,
        )
        assert isinstance(test1, ProcessedMessage)

        res1 = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(post_hash)
        )
        assert res1
        assert res1.item_hash == post_hash

        # 2) process forget message
        test2 = await message_handler.process(
            session=session,
            pending_message=m2,
        )
        assert isinstance(test2, ProcessedMessage)
        res2 = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(post_hash)
        )
        assert res2 is None

        # 3) process post message confirmation (discarding it)
        test3 = await message_handler.process(
            session=session,
            pending_message=m3,
        )
        assert isinstance(test3, RejectedMessage)
        assert test3.error_code == ErrorCode.FORGOTTEN_DUPLICATE

        res3 = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(post_hash)
        )

        assert res3 is None

        res4 = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(post_hash)
        )
        assert res4
