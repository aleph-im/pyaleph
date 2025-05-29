import datetime as dt
from decimal import Decimal

import pytest
import pytest_asyncio
from aleph_message.models import Chain, ItemType, MessageType, PaymentType

from aleph.db.accessors.messages import get_message_status
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.balances import AlephBalanceDb
from aleph.db.models.chains import ChainTxDb
from aleph.db.models.cron_jobs import CronJobDb
from aleph.db.models.files import (
    FilePinType,
    GracePeriodFilePinDb,
    MessageFilePinDb,
    StoredFileDb,
)
from aleph.db.models.messages import MessageDb, MessageStatusDb
from aleph.jobs.cron.balance_job import BalanceCronJob
from aleph.toolkit.constants import STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT, MiB
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import MessageStatus


@pytest.fixture
def balance_job(session_factory: DbSessionFactory) -> BalanceCronJob:
    return BalanceCronJob(session_factory=session_factory)


@pytest.fixture
def now():
    return utc_now()


def create_cron_job(id, now):
    """Create a cron job entry for testing."""
    return CronJobDb(
        id=id,
        interval=1,
        last_run=now - dt.timedelta(hours=1),
    )


def create_wallet(address, balance, now):
    """Create a wallet with the specified balance."""
    return AlephBalanceDb(
        address=address,
        balance=Decimal(str(balance)),
        last_update=now,
        chain=Chain.ETH,
        eth_height=STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT,
    )


def create_store_message(
    item_hash,
    sender,
    file_hash,
    now,
    size=30 * MiB,
    status=MessageStatus.PROCESSED,
):
    """Create a store message with associated file and status."""
    message = MessageDb(
        item_hash=item_hash,
        sender=sender,
        chain=Chain.ETH,
        type=MessageType.store,
        time=now,
        item_type=ItemType.ipfs,
        signature=f"sig_{item_hash[:8]}",
        size=size,
        content={
            "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "time": 1645794065.439,
            "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
            "reason": "None",
            "type": "TEST",
            "item_hash": file_hash,
            "item_type": ItemType.ipfs.value,
        },
    )

    file = StoredFileDb(
        hash=file_hash,
        size=size,
        type=FileType.FILE,
    )

    if status == MessageStatus.PROCESSED:
        file_pin = MessageFilePinDb(
            item_hash=item_hash,
            file_hash=file_hash,
            type=FilePinType.MESSAGE,
            owner=sender,
            created=now,
        )
    elif status == MessageStatus.REMOVING:
        file_pin = GracePeriodFilePinDb(
            item_hash=item_hash,
            file_hash=file_hash,
            type=FilePinType.GRACE_PERIOD,
            owner=sender,
            created=now,
            delete_by=now + dt.timedelta(hours=24),
        )

    message_status = MessageStatusDb(
        item_hash=item_hash,
        status=status,
        reception_time=now,
    )

    return message, file, file_pin, message_status


def create_message_cost(owner, item_hash, cost_hold):
    """Create a cost record for a message."""
    return AccountCostsDb(
        owner=owner,
        item_hash=item_hash,
        type=CostType.STORAGE,
        name="store",
        payment_type=PaymentType.hold,
        cost_hold=Decimal(str(cost_hold)),
        cost_stream=Decimal("0.0"),
    )


def add_chain_confirmation(message, height, now):
    """Add a chain confirmation with specified height to a message."""
    chain_confirm = ChainTxDb(
        hash="0x111",
        chain=Chain.ETH,
        height=height,
        datetime=now,
        publisher="0xabadbabe",
        protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
        protocol_version=1,
        content="Qmsomething",
    )
    message.confirmations = [chain_confirm]
    return message


@pytest_asyncio.fixture
async def fixture_base_data(session_factory, now):
    """Create base data that can be used by multiple tests."""
    # Create cron job
    cron_job = create_cron_job("balance_check_base", now)

    with session_factory() as session:
        session.add(cron_job)
        session.commit()

    return {"cron_job_name": "balance_check_base"}


@pytest_asyncio.fixture
async def fixture_message_for_removal(session_factory, now, fixture_base_data):
    """
    Setup for testing a message that should be marked for removal due to insufficient balance.
    """
    wallet_address = "0xtestaddress1"
    message_hash = "abcd1234" * 4
    file_hash = "1234" * 16

    # Create wallet with low balance
    wallet = create_wallet(wallet_address, "10.0", now)

    # Create message and associated records
    message, file, file_pin, message_status = create_store_message(
        message_hash, wallet_address, file_hash, now
    )

    # Add message cost (more than wallet balance)
    message_cost = create_message_cost(wallet_address, message_hash, "15.0")

    # Add chain confirmation with height above cutoff
    message = add_chain_confirmation(
        message, STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT + 1000, now
    )

    with session_factory() as session:
        session.add_all([message])
        session.commit()

        session.add_all([wallet, file, file_pin, message_status, message_cost])
        session.commit()

    return {
        "wallet_address": wallet_address,
        "message_hash": message_hash,
    }


@pytest_asyncio.fixture
async def fixture_message_below_cutoff(session_factory, now, fixture_base_data):
    """
    Setup for testing a message that should not be marked for removal
    because its height is below the cutoff.
    """
    wallet_address = "0xtestaddress2"
    message_hash = "bcde2345" * 4
    file_hash = "1234" * 16

    # Create wallet with low balance
    wallet = create_wallet(wallet_address, "5.0", now)

    # Create message and associated records
    message, file, file_pin, message_status = create_store_message(
        message_hash, wallet_address, file_hash, now
    )

    # Add message cost (more than wallet balance)
    message_cost = create_message_cost(wallet_address, message_hash, "10.0")

    # Add chain confirmation with height BELOW cutoff
    message = add_chain_confirmation(
        message, STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT - 1000, now
    )

    with session_factory() as session:
        session.add_all([message])
        session.commit()

        session.add_all([wallet, file, file_pin, message_status, message_cost])
        session.commit()

    return {
        "wallet_address": wallet_address,
        "message_hash": message_hash,
    }


@pytest_asyncio.fixture
async def fixture_message_for_recovery(session_factory, now, fixture_base_data):
    """
    Setup for testing a message that should be recovered from REMOVING status
    because the wallet balance is now sufficient.
    """
    wallet_address = "0xtestaddress3"
    message_hash = "cdef3456" * 4
    file_hash = "1234" * 16

    # Create wallet with sufficient balance
    wallet = create_wallet(wallet_address, "50.0", now)

    # Create message and associated records with REMOVING status
    message, file, file_pin, _ = create_store_message(
        message_hash,
        wallet_address,
        file_hash,
        now,
        status=MessageStatus.REMOVING,  # Set status to REMOVING
    )

    # Override the message status to ensure it's REMOVING
    message_status = MessageStatusDb(
        item_hash=message_hash,
        status=MessageStatus.REMOVING,
        reception_time=now,
    )

    # Add message cost (less than wallet balance now)
    message_cost = create_message_cost(wallet_address, message_hash, "20.0")

    # Add chain confirmation with height above cutoff
    message = add_chain_confirmation(
        message, STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT + 2000, now
    )

    with session_factory() as session:
        session.add_all([message])
        session.commit()

        session.add_all([wallet, file, file_pin, message_status, message_cost])
        session.commit()

    return {
        "wallet_address": wallet_address,
        "message_hash": message_hash,
    }


@pytest.mark.asyncio
async def test_balance_job_marks_messages_for_removal(
    session_factory, balance_job, fixture_message_for_removal, now
):
    """Test that the balance job marks messages for removal when balance is insufficient."""
    # Get the cron job
    with session_factory() as session:
        cron_job = session.query(CronJobDb).filter_by(id="balance_check_base").one()

    # Run the balance job
    await balance_job.run(now, cron_job)

    # Check if the message was marked for removal
    with session_factory() as session:
        # Check message status changed to REMOVING
        message_status = get_message_status(
            session=session, item_hash=fixture_message_for_removal["message_hash"]
        )
        assert message_status is not None
        assert message_status.status == MessageStatus.REMOVING

        # Check if a grace period was added to the file pin
        grace_period_pins = (
            session.query(GracePeriodFilePinDb)
            .filter_by(item_hash=fixture_message_for_removal["message_hash"])
            .all()
        )

        assert len(grace_period_pins) == 1
        assert grace_period_pins[0].delete_by is not None

        # Delete should be around 25 hours in the future (24+1 as specified in the code)
        delete_by = grace_period_pins[0].delete_by
        time_diff = delete_by - now
        assert (
            24.5 <= time_diff.total_seconds() / 3600 <= 25.5
        )  # Between 24.5 and 25.5 hours


@pytest.mark.asyncio
async def test_balance_job_ignores_messages_below_cutoff_height(
    session_factory, balance_job, fixture_message_below_cutoff, now
):
    """Test that the balance job ignores messages with height below the cutoff."""
    # Get the cron job
    with session_factory() as session:
        cron_job = session.query(CronJobDb).filter_by(id="balance_check_base").one()

    # Run the balance job
    await balance_job.run(now, cron_job)

    # Check that the message was NOT marked for removal (still PROCESSED)
    with session_factory() as session:
        message_status = get_message_status(
            session=session, item_hash=fixture_message_below_cutoff["message_hash"]
        )
        assert message_status is not None
        assert message_status.status == MessageStatus.PROCESSED

        # Check no grace period was added
        grace_period_pins = (
            session.query(GracePeriodFilePinDb)
            .filter_by(item_hash=fixture_message_below_cutoff["message_hash"])
            .all()
        )

        assert len(grace_period_pins) == 0


@pytest.mark.asyncio
async def test_balance_job_recovers_messages_with_sufficient_balance(
    session_factory, balance_job, fixture_message_for_recovery, now
):
    """Test that the balance job recovers messages with REMOVING status when balance is sufficient."""
    # Get the cron job
    with session_factory() as session:
        cron_job = session.query(CronJobDb).filter_by(id="balance_check_base").one()

    # Run the balance job
    await balance_job.run(now, cron_job)

    # Check that the message was recovered (marked as PROCESSED again)
    with session_factory() as session:
        message_status = get_message_status(
            session=session, item_hash=fixture_message_for_recovery["message_hash"]
        )
        assert message_status is not None
        assert message_status.status == MessageStatus.PROCESSED

        # Check grace period was updated to null (no deletion date)
        grace_period_pins = (
            session.query(GracePeriodFilePinDb)
            .filter_by(item_hash=fixture_message_for_recovery["message_hash"])
            .all()
        )

        assert len(grace_period_pins) == 0
