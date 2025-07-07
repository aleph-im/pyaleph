import datetime as dt
from typing import cast

import pytest
from configmanager import Config
from sqlalchemy import select

from aleph.db.models import PendingMessageDb
from aleph.db.models.messages import ForgottenMessageDb, MessageDb
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

        result1 = await session.execute(
            select(MessageDb).where(MessageDb.item_hash == post_hash)
        )
        res1 = cast(MessageDb, result1.scalars().first())
        assert res1.item_hash == post_hash

        # 2) process forget message
        test2 = await message_handler.process(
            session=session,
            pending_message=m2,
        )
        assert isinstance(test2, ProcessedMessage)

        result2 = await session.execute(
            select(MessageDb).where(MessageDb.item_hash == post_hash)
        )
        res2 = result2.scalars().first()
        assert res2 is None

        # 3) process post message confirmation (discarding it)
        test3 = await message_handler.process(
            session=session,
            pending_message=m3,
        )
        assert isinstance(test3, RejectedMessage)
        assert test3.error_code == ErrorCode.FORGOTTEN_DUPLICATE

        result3 = await session.execute(
            select(MessageDb).where(MessageDb.item_hash == post_hash)
        )
        res3 = result3.scalars().first()
        assert res3 is None

        result4 = await session.execute(
            select(ForgottenMessageDb).where(ForgottenMessageDb.item_hash == post_hash)
        )
        res4 = cast(ForgottenMessageDb, result4.scalars().first())
        assert res4
