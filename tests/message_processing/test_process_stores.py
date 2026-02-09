import datetime as dt
import json
from decimal import Decimal
from typing import AsyncIterable, Mapping, Optional
from unittest.mock import AsyncMock, patch

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType, StoreContent
from configmanager import Config

from aleph.db.accessors.files import get_message_file_pin
from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.db.models import (
    AlephBalanceDb,
    AlephCreditBalanceDb,
    MessageDb,
    MessageStatusDb,
    PendingMessageDb,
)
from aleph.handlers.content.store import StoreMessageHandler
from aleph.handlers.message_handler import MessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.services.cost import get_total_and_detailed_costs_from_db
from aleph.services.storage.engine import StorageEngine
from aleph.storage import StorageService
from aleph.toolkit.constants import (
    MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP,
    STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP,
)
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_processing_result import ProcessedMessage
from aleph.types.message_status import (
    InsufficientBalanceException,
    InsufficientCreditException,
    MessageStatus,
)


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


@pytest.fixture()
def fixture_ipfs_store_message() -> PendingMessageDb:
    return PendingMessageDb(
        item_hash="af2e19894099d954f3d1fa274547f62484bc2d93964658547deecc70316acc72",
        type=MessageType.store,
        chain=Chain.ETH,
        sender="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        signature="0xb9d164e6e43a8fcd341abc01eda47bed0333eaf480e888f2ed2ae0017048939d18850a33352e7281645e95e8673bad733499b6a8ce4069b9da9b9a79ddc1a0b31b",
        item_type=ItemType.inline,
        item_content='{"address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106", "time": 1665478676.6585264, "item_type": "ipfs", "item_hash": "QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ", "mime_type": "text/plain"}',
        time=timestamp_to_datetime(STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1),
        channel=Channel("TEST"),
        check_message=True,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        fetched=False,
        reception_time=timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
        ),
    )


@pytest.fixture()
def fixture_store_message_with_cost() -> PendingMessageDb:
    return PendingMessageDb(
        item_hash="af2e19894099d954f3d1fa274547f62484bc2d93964658547deecc70316acc72",
        type=MessageType.store,
        chain=Chain.ETH,
        sender="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        signature="0xb9d164e6e43a8fcd341abc01eda47bed0333eaf480e888f2ed2ae0017048939d18850a33352e7281645e95e8673bad733499b6a8ce4069b9da9b9a79ddc1a0b31b",
        item_type=ItemType.inline,
        item_content='{"address": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106", "time": 1665478676.6585264, "item_type": "storage", "item_hash": "c25b0525bc308797d3e35763faf5c560f2974dab802cb4a734ae4e9d1040319e", "mime_type": "text/plain"}',
        time=timestamp_to_datetime(STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1),
        channel=Channel("TEST"),
        check_message=True,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        fetched=False,
        reception_time=timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
        ),
    )


@pytest.fixture
def create_message_db(mocker):
    def _create_message(
        item_hash="test-hash",
        address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1,
        item_type=ItemType.ipfs,
        item_content_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
    ):
        content = StoreContent(
            address=address,
            time=time,
            item_type=item_type,
            item_hash=item_content_hash,
        )

        message = mocker.MagicMock(spec=MessageDb)
        message.item_hash = item_hash
        message.type = MessageType.store
        message.chain = Chain.ETH
        message.sender = address
        message.signature = "0xsignature"
        message.item_type = ItemType.inline
        message.item_content = json.dumps(content.model_dump())
        message.parsed_content = content
        message.time = timestamp_to_datetime(time)
        message.channel = Channel("TEST")

        return message

    return _create_message


# TODO: remove duplication of this class
class MockStorageEngine(StorageEngine):
    def __init__(self, files: Mapping[str, bytes]):
        self.files = files

    async def read(self, filename: str) -> Optional[bytes]:
        return self.files.get(filename)

    async def read_iterator(
        self, filename: str, chunk_size: int = 1024 * 1024
    ) -> Optional[AsyncIterable[bytes]]:
        content = await self.read(filename)
        if content is None:
            return None

        async def _read_iterator():
            for i in range(0, len(content), chunk_size):
                yield content[i : i + chunk_size]

        return _read_iterator()

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
        processed_message = await message_handler.process(
            session=session, pending_message=fixture_store_message
        )
        session.commit()
        assert isinstance(processed_message, ProcessedMessage)
        store_content = StoreContent.model_validate(processed_message.message.content)

        cost, _ = get_total_and_detailed_costs_from_db(
            session=session,
            content=store_content,
            item_hash=fixture_store_message.item_hash,
        )

        assert cost == Decimal("0.000004450480138778")


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

    file_hash = "c25b0525bc308797d3e35763faf5c560f2974dab802cb4a734ae4e9d1040319e"
    file_content = b"Hello Aleph.im"

    storage_service = StorageService(
        storage_engine=MockStorageEngine(files={file_hash: file_content}),
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


@pytest.mark.asyncio
async def test_process_store_with_not_enough_balance(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_store_message_with_cost: PendingMessageDb,
):
    # Create a large file (> 25 MiB)
    large_file_content = b"X" * (26 * 1024 * 1024)  # 26 MiB

    storage_service = StorageService(
        storage_engine=MockStorageEngine(
            files={
                "c25b0525bc308797d3e35763faf5c560f2974dab802cb4a734ae4e9d1040319e": large_file_content
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
        # NOTE: Account balance is 0 at this point
        with pytest.raises(InsufficientBalanceException):
            await message_handler.process(
                session=session, pending_message=fixture_store_message_with_cost
            )


@pytest.mark.asyncio
async def test_process_store_small_file_no_balance_required(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_store_message_with_cost: PendingMessageDb,
):
    """
    Test that a STORE message with a small file (<=25MiB) can be processed
    even with insufficient balance.
    """
    # Create a small file (<= 25 MiB)
    small_file_content = b"X" * (25 * 1024 * 1024)  # 25 MiB

    storage_service = StorageService(
        storage_engine=MockStorageEngine(
            files={
                "c25b0525bc308797d3e35763faf5c560f2974dab802cb4a734ae4e9d1040319e": small_file_content
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
        # NOTE: Account balance is 0 at this point, but since the file is small
        # it should still be processed
        await message_handler.process(
            session=session, pending_message=fixture_store_message_with_cost
        )
        session.commit()

        # Verify that the message was processed successfully
        message_db = get_message_by_item_hash(
            session=session,
            item_hash=ItemHash(fixture_store_message_with_cost.item_hash),
        )
        assert message_db is not None

        file_pin = get_message_file_pin(
            session=session,
            item_hash=ItemHash(fixture_store_message_with_cost.item_hash),
        )
        assert file_pin is not None


# Tests specifically for the pre_check_balance method


@pytest.mark.asyncio
async def test_pre_check_balance_free_store_message(
    mocker, session_factory, mock_config
):
    """Test that messages sent before the cost deadline are free."""
    ipfs_service = mocker.AsyncMock()
    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        # Create a message with timestamp before the deadline
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP - 1
        )
        content = StoreContent(
            address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP - 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Should return None without checking balance
        result = await store_handler.pre_check_balance(session, message)
        assert result is None

        # Verify that get_ipfs_size was not called
        assert not ipfs_service.get_ipfs_size.called


@pytest.mark.asyncio
async def test_pre_check_balance_small_ipfs_file(mocker, session_factory, mock_config):
    """Test that small IPFS files (<=25MiB) don't require balance."""
    small_file_size = int(
        MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 0.9
    )  # 90% of max free size

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=small_file_size)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
        )
        content = StoreContent(
            address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Should return None without checking balance
        result = await store_handler.pre_check_balance(session, message)
        assert result is None

        # Verify that get_ipfs_size was called with correct hash
        ipfs_service.get_ipfs_size.assert_called_once_with(
            "QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ"
        )


@pytest.mark.asyncio
async def test_pre_check_balance_large_ipfs_file_insufficient_balance(
    mocker,
    session_factory,
    mock_config,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Test that large IPFS files (>25MiB) require sufficient balance."""
    large_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 2)  # 2x max free size

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=large_file_size)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
        )
        content = StoreContent(
            address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # No balance in the account
        with pytest.raises(InsufficientBalanceException) as exc_info:
            await store_handler.pre_check_balance(session, message)

        # Verify exception contains correct balance information
        assert exc_info.value.balance == Decimal(0)
        assert exc_info.value.required_balance > Decimal(0)

        # Verify that get_ipfs_size was called with correct hash
        ipfs_service.get_ipfs_size.assert_called_once_with(
            "QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ"
        )


@pytest.mark.asyncio
async def test_pre_check_balance_large_ipfs_file_sufficient_balance(
    mocker,
    session_factory,
    mock_config,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Test that large IPFS files (>25MiB) with sufficient balance pass the check."""
    large_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 2)  # 2x max free size

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=large_file_size)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        # Create a message with a large file
        address = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
        )
        content = StoreContent(
            address=address,
            time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Add sufficient balance for the sender
        session.add(
            AlephBalanceDb(
                address=address,
                chain=Chain.ETH,
                balance=Decimal(1000),  # Large enough to cover any costs
                eth_height=100,
            )
        )
        session.commit()

        # Should pass the balance check
        result = await store_handler.pre_check_balance(session, message)
        assert result is None

        # Verify that get_ipfs_size was called with correct hash
        ipfs_service.get_ipfs_size.assert_called_once_with(
            "QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ"
        )


@pytest.mark.asyncio
async def test_pre_check_balance_non_ipfs_file(mocker, session_factory, mock_config):
    """Test that non-IPFS files don't require a balance check."""
    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock()

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        # Create a message with a non-IPFS file type
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
        )
        content = StoreContent(
            address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.storage,  # Not IPFS
            item_hash="af2e19894099d954f3d1fa274547f62484bc2d93964658547deecc70316acc72",
        )
        message.parsed_content = content

        # Should return None without checking balance
        result = await store_handler.pre_check_balance(session, message)
        assert result is None

        # Verify that get_ipfs_size was not called
        assert not ipfs_service.get_ipfs_size.called


@pytest.mark.asyncio
async def test_pre_check_balance_ipfs_disabled(mocker, session_factory):
    """Test that when IPFS is disabled, no balance check is performed."""
    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock()

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    # Create a mock config with IPFS disabled
    mock_config = mocker.MagicMock(spec=Config)
    ipfs_config = mocker.MagicMock()
    ipfs_config.enabled.value = False
    mock_config.ipfs = ipfs_config

    # Patch the get_config function to return our mock config
    with patch("aleph.handlers.content.store.get_config", return_value=mock_config):
        with session_factory() as session:
            message = mocker.MagicMock(spec=MessageDb)
            message.time = timestamp_to_datetime(
                STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
            )
            content = StoreContent(
                address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
                time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1,
                item_type=ItemType.ipfs,
                item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
            )
            message.parsed_content = content

            # Should return None without checking balance
            result = await store_handler.pre_check_balance(session, message)
            assert result is None

            # Verify that get_ipfs_size was not called
            assert not ipfs_service.get_ipfs_size.called


@pytest.mark.asyncio
async def test_pre_check_balance_ipfs_size_none(mocker, session_factory, mock_config):
    """Test handling when get_ipfs_size returns None (file not found)."""
    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=None)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
        )
        content = StoreContent(
            address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Should return None as no size means no cost
        result = await store_handler.pre_check_balance(session, message)
        assert result is None

        # Verify that get_ipfs_size was called with correct hash
        ipfs_service.get_ipfs_size.assert_called_once_with(
            "QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ"
        )


@pytest.mark.asyncio
async def test_pre_check_balance_with_existing_costs(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_ipfs_store_message: PendingMessageDb,
):
    """Test that existing costs for an address are considered."""
    small_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE)  # max free size
    small_file_content = b"X" * small_file_size  # 25 MiB
    large_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 2)  # 2x max free size

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=small_file_size)
    ipfs_service.ipfs_client.files.stat = AsyncMock(
        return_value={"Type": "file", "Size": len(small_file_content)}
    )

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )

    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        # Create a message with a large file
        address = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(
            STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1
        )
        content = StoreContent(
            address=address,
            time=STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.ipfs,
            item_hash=ItemHash("QmacDVDroxPVY1enhckVco1rTBziwC8hjf731apEKr3QoG"),
        )
        message.parsed_content = content
        message.item_hash = (
            "e70e5e5d3f080393c6274180f49b649980fda63215a0d1492e728b5472f9405e"
        )

        # Add balance that's enough for the new file but not for both
        session.add(
            AlephBalanceDb(
                address=address,
                chain=Chain.ETH,
                balance=Decimal("15.0"),  # Not enough for existing + new
                eth_height=100,
            )
        )
        session.commit()

        # Disable signature verification
        signature_verifier = mocker.AsyncMock()
        message_handler = MessageHandler(
            signature_verifier=signature_verifier,
            storage_service=storage_service,
            config=mock_config,
        )

        # Patch the get_hash_content function to return our expected result and be able to process the message
        with patch(
            "aleph.storage.StorageService.get_hash_content",
            return_value=small_file_content,
        ):
            # Process first message to add existing costs
            await message_handler.process(
                session=session, pending_message=fixture_ipfs_store_message
            )
            session.commit()

            ipfs_service.get_ipfs_size = AsyncMock(return_value=large_file_size)

            # Should fail the balance check
            with pytest.raises(InsufficientBalanceException) as exc_info:
                await store_handler.pre_check_balance(session, message)

            # Verify exception contains correct balance information
            assert exc_info.value.balance == Decimal("15.0")
            assert exc_info.value.required_balance > Decimal("15.0")

            # Verify that get_ipfs_size was called with correct hash
            ipfs_service.get_ipfs_size.assert_called_once_with(
                "QmacDVDroxPVY1enhckVco1rTBziwC8hjf731apEKr3QoG"
            )


# Tests for credit-only STORE messages (after STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP)


@pytest.mark.asyncio
async def test_new_store_message_requires_credits(
    mocker,
    session_factory,
    mock_config,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Test that new STORE messages (after cutoff) automatically use credit payment and require credits."""
    small_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 0.5)  # 50% of max

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=small_file_size)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        address = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"
        # Create a message after the credit-only cutoff
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP + 1)
        message.confirmations = []
        message.item_hash = "test-hash"
        content = StoreContent(
            address=address,
            time=STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Should raise InsufficientCreditException (no credits available)
        with pytest.raises(InsufficientCreditException):
            await store_handler.pre_check_balance(session, message)


@pytest.mark.asyncio
async def test_new_store_message_with_sufficient_credits(
    mocker,
    session_factory,
    mock_config,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Test that new STORE messages with sufficient credits pass."""
    small_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 0.5)  # 50% of max

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=small_file_size)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        address = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"
        # Create a message after the credit-only cutoff
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP + 1)
        message.confirmations = []
        message.item_hash = "test-hash"
        content = StoreContent(
            address=address,
            time=STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Add sufficient credit balance (enough for 1 day of storage)
        session.add(
            AlephCreditBalanceDb(
                address=address,
                balance=1000000000,  # Large enough to cover 1 day of storage
            )
        )
        session.commit()

        # Should pass the balance check
        result = await store_handler.pre_check_balance(session, message)
        assert result is None


@pytest.mark.asyncio
async def test_legacy_store_message_uses_hold_payment(
    mocker, session_factory, mock_config
):
    """Test that legacy STORE messages (before cutoff) use hold payment with 25MB exception."""
    small_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 0.5)  # 50% of max

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=small_file_size)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        # Create a message BEFORE the credit-only cutoff (legacy message)
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP - 1)
        message.confirmations = []
        content = StoreContent(
            address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            time=STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP - 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Should pass (legacy messages use hold and small file exception applies)
        result = await store_handler.pre_check_balance(session, message)
        assert result is None


@pytest.mark.asyncio
async def test_new_store_small_file_still_requires_credits(
    mocker,
    session_factory,
    mock_config,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Test that new STORE messages with small files still require credits (no free exception)."""
    small_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 0.5)  # 50% of max

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=small_file_size)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        address = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"
        # Create a message after the credit-only cutoff with a small file
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP + 1)
        message.confirmations = []
        message.item_hash = "test-hash"
        content = StoreContent(
            address=address,
            time=STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP + 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Should raise InsufficientCreditException even for small files (no free exception after cutoff)
        with pytest.raises(InsufficientCreditException):
            await store_handler.pre_check_balance(session, message)


@pytest.mark.asyncio
async def test_legacy_store_small_file_no_balance_required(
    mocker, session_factory, mock_config
):
    """Test that legacy STORE messages with small files don't require balance (25MB exception)."""
    small_file_size = int(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE * 0.9)  # 90% of max

    ipfs_service = mocker.AsyncMock()
    ipfs_service.get_ipfs_size = AsyncMock(return_value=small_file_size)

    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    store_handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
    )

    with session_factory() as session:
        # Create a legacy message with a small file
        message = mocker.MagicMock(spec=MessageDb)
        message.time = timestamp_to_datetime(STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP - 1)
        message.confirmations = []
        content = StoreContent(
            address="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            time=STORE_CREDIT_ONLY_CUTOFF_TIMESTAMP - 1,
            item_type=ItemType.ipfs,
            item_hash="QmWVxvresoeadRbCeG4BmvsoSsqHV7VwUNuGK6nUCKKFGQ",
        )
        message.parsed_content = content

        # Should pass without checking balance (25MB exception applies for legacy)
        result = await store_handler.pre_check_balance(session, message)
        assert result is None
