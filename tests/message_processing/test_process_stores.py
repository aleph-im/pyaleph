import datetime as dt
import json
from typing import Mapping, Optional

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from configmanager import Config

from aleph.db.accessors.files import get_message_file_pin
from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.db.models import MessageStatusDb, PendingMessageDb
from aleph.handlers.content.store import StoreMessageHandler
from aleph.handlers.message_handler import MessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.services.storage.engine import StorageEngine
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus


@pytest.fixture()
def fixture_store_message() -> PendingMessageDb:
    return PendingMessageDb(
        item_hash="af2e19894099d954f3d1fa274547f62484bc2d93964658547deecc70316acc72",
        type=MessageType.store,
        chain=Chain.ETH,
        sender="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        signature="0xb9d164e6e43a8fcd341abc01eda47bed0333eaf480e888f2ed2ae0017048939d18850a33352e7281645e95e8673bad733499b6a8ce4069b9da9b9a79ddc1a0b31b",
        item_type=ItemType.inline,
        item_content='{"address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106", "time": 1665478676.6585264, "item_type": "storage", "item_hash": "c25b0525bc308797d3e35763faf5c560f2974dab802cb4a734ae4e9d1040319e", "mime_type": "text/plain"}',
        time=timestamp_to_datetime(1665478676.658627),
        channel=Channel("TEST"),
        check_message=True,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        fetched=False,
        reception_time=timestamp_to_datetime(1665478677),
    )


# TODO: remove duplication of this class
class MockStorageEngine(StorageEngine):
    def __init__(self, files: Mapping[str, bytes]):
        self.files = files

    async def read(self, filename: str) -> Optional[bytes]:
        return self.files.get(filename)

    async def write(self, filename: str, content: bytes):
        pass

    async def delete(self, filename: str):
        pass

    async def exists(self, filename: str) -> bool:
        return filename in self.files


@pytest.mark.asyncio
async def test_process_store(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_store_message: PendingMessageDb,
):
    storage_service = StorageService(
        storage_engine=MockStorageEngine(
            files={
                "c25b0525bc308797d3e35763faf5c560f2974dab802cb4a734ae4e9d1040319e": b"Hello Aleph.im"
            }
        ),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )
    # Disable signature verification
    signature_verifier = mocker.AsyncMock()
    message_handler = MessageHandler(
        signature_verifier=signature_verifier,
        storage_service=storage_service,
        config=mock_config,
    )

    with session_factory() as session:
        await message_handler.process(
            session=session, pending_message=fixture_store_message
        )
        session.commit()


@pytest.mark.asyncio
async def test_process_store_no_signature(
    mocker,
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_store_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    Test that a STORE message with no signature (i.e., coming from a smart contract)
    can be processed.
    """

    fixture_store_message.check_message = False
    fixture_store_message.signature = None
    fixture_store_message.fetched = True
    assert fixture_store_message.item_content  # for mypy
    content = json.loads(fixture_store_message.item_content)
    fixture_store_message.content = content

    with session_factory() as session:
        session.add(fixture_store_message)
        session.add(
            MessageStatusDb(
                item_hash=fixture_store_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=fixture_store_message.reception_time,
            )
        )
        session.commit()

    storage_service = StorageService(
        storage_engine=MockStorageEngine(
            files={
                "c25b0525bc308797d3e35763faf5c560f2974dab802cb4a734ae4e9d1040319e": b"Hello Aleph.im"
            }
        ),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )
    message_processor.message_handler.storage_service = storage_service
    storage_handler = message_processor.message_handler.content_handlers[
        MessageType.store
    ]
    assert isinstance(storage_handler, StoreMessageHandler)
    storage_handler.storage_service = storage_service

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    with session_factory() as session:
        message_db = get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_store_message.item_hash)
        )

        assert message_db is not None
        assert message_db.signature is None

        file_pin = get_message_file_pin(
            session=session, item_hash=ItemHash(fixture_store_message.item_hash)
        )
        assert file_pin is not None
        assert file_pin.file_hash == content["item_hash"]
