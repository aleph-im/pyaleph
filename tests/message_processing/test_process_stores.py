from typing import Optional, Dict, Mapping

import pytest
from aleph_message.models import Chain, MessageType, ItemType
from configmanager import Config

from aleph.db.models import PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.services.storage.engine import StorageEngine
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory


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
    mocker, mock_config: Config, session_factory: DbSessionFactory, fixture_store_message: PendingMessageDb
):
    storage_service = StorageService(
        storage_engine=MockStorageEngine(
            files={
                "c25b0525bc308797d3e35763faf5c560f2974dab802cb4a734ae4e9d1040319e": b"Hello Aleph.im"
            }
        ),
        ipfs_service=mocker.AsyncMock(),
    )
    chain_service = mocker.AsyncMock()
    message_handler = MessageHandler(
        session_factory=session_factory,
        chain_service=chain_service,
        storage_service=storage_service,
        config=mock_config,
    )

    await message_handler.fetch_and_process_one_message_db(fixture_store_message)
