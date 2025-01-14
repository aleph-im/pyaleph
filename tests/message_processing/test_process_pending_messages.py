import pytest
from configmanager import Config

from aleph.db.models import PendingMessageDb
from aleph.handlers.message_handler import MessagePublisher
from aleph.storage import StorageService
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageOrigin

from .load_fixtures import load_fixture_message


@pytest.mark.asyncio
async def test_pending_message(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    test_storage_service: StorageService,
):
    message = load_fixture_message("test-data-pending-messaging.json")

    message_publisher = MessagePublisher(
        session_factory=session_factory,
        storage_service=test_storage_service,
        config=mock_config,
        pending_message_exchange=mocker.AsyncMock(),
    )

    test1 = await message_publisher.add_pending_message(
        message_dict=message,
        reception_time=utc_now(),
        origin=MessageOrigin.P2P,
    )
    assert test1

    test2 = await message_publisher.add_pending_message(
        message_dict=message,
        reception_time=utc_now(),
        origin=MessageOrigin.P2P,
    )
    assert test2

    assert test2.content == test1.content
    assert test2.reception_time == test1.reception_time

    with session_factory() as session:
        pending_messages = session.query(PendingMessageDb).count()
        assert pending_messages == 1
