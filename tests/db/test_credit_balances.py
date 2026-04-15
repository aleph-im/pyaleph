import datetime as dt
import time
from decimal import Decimal
from typing import Any, Dict, List

from sqlalchemy import select
from sqlalchemy import update as sql_update

from aleph.db.accessors.balances import (
    count_address_credit_history,
    get_address_credit_history,
    get_consumed_credits_by_resource,
    get_credit_balance,
    get_credit_balance_with_details,
    get_resource_consumed_credits,
    get_total_consumed_credits,
    update_credit_balances_distribution,
    update_credit_balances_expense,
    update_credit_balances_transfer,
    validate_credit_transfer_balance,
)
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortByCreditHistory, SortOrder


def test_update_credit_balances_distribution(session_factory: DbSessionFactory):
    """Test direct database insertion for credit distribution messages."""
    credits_list = [
        {
            "address": "0x123",
            "amount": 1000,
            "price": "0.5",
            "tx_hash": "0xabc123",
            "provider": "test_provider",
            "expiration": 1700000000000,  # timestamp in ms
            "origin": "test_origin",
            "ref": "test_ref",
            "payment_method": "test_payment",
        }
    ]

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=credits_list,
            token="TEST_TOKEN",
            chain="ETH",
            message_hash="msg_hash_123",
            message_timestamp=message_timestamp,
        )
        session.commit()

        # Verify credit history record was inserted
        credit_record = (
            session.query(AlephCreditHistoryDb)
            .filter_by(address="0x123", credit_ref="msg_hash_123")
            .first()
        )

        assert credit_record is not None
        assert credit_record.address == "0x123"
        assert credit_record.amount == 10000000  # 1000 * 10000 multiplier
        assert credit_record.price == Decimal("0.5")
        assert credit_record.tx_hash == "0xabc123"
        assert credit_record.token == "TEST_TOKEN"
        assert credit_record.chain == "ETH"
        assert credit_record.provider == "test_provider"
        assert credit_record.origin == "test_origin"
        assert credit_record.origin_ref == "test_ref"
        assert credit_record.payment_method == "test_payment"
        assert credit_record.credit_ref == "msg_hash_123"
        assert credit_record.credit_index == 0
        assert credit_record.expiration_date is not None
        assert credit_record.message_timestamp == message_timestamp


def test_update_credit_balances_expense(session_factory: DbSessionFactory):
    """Test direct database insertion for credit expense messages."""
    credits_list = [
        {
            "address": "0x456",
            "amount": 500,
            "ref": "expense_ref",
        }
    ]

    message_timestamp = dt.datetime(2023, 1, 2, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_expense(
            session=session,
            credits_list=credits_list,
            message_hash="expense_msg_789",
            message_timestamp=message_timestamp,
        )
        session.commit()

        # Verify expense record was inserted
        expense_record = (
            session.query(AlephCreditHistoryDb)
            .filter_by(address="0x456", credit_ref="expense_msg_789")
            .first()
        )

        assert expense_record is not None
        assert expense_record.address == "0x456"
        assert expense_record.amount == -5000000  # -500 * 10000 multiplier
        assert expense_record.price is None
        assert expense_record.tx_hash is None
        assert expense_record.token is None
        assert expense_record.chain is None
        assert expense_record.provider == "ALEPH"
        assert expense_record.origin is None
        assert expense_record.origin_ref == "expense_ref"
        assert expense_record.payment_method == "credit_expense"
        assert expense_record.credit_ref == "expense_msg_789"
        assert expense_record.credit_index == 0
        assert expense_record.expiration_date is None
        assert expense_record.message_timestamp == message_timestamp


def test_update_credit_balances_expense_with_new_fields(
    session_factory: DbSessionFactory,
):
    """Test direct database insertion for credit expense messages with new fields."""
    credits_list = [
        {
            "address": "0x456",
            "amount": 500,
            "ref": "expense_ref",
            "execution_id": "exec_12345",
            "node_id": "node_67890",
            "price": "0.001",
            "time": 1640995200000,  # This will be skipped for now
        }
    ]

    message_timestamp = dt.datetime(2023, 1, 2, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_expense(
            session=session,
            credits_list=credits_list,
            message_hash="expense_msg_with_fields_789",
            message_timestamp=message_timestamp,
        )
        session.commit()

        # Verify expense record was inserted with new field mappings
        expense_record = (
            session.query(AlephCreditHistoryDb)
            .filter_by(address="0x456", credit_ref="expense_msg_with_fields_789")
            .first()
        )

        assert expense_record is not None
        assert expense_record.address == "0x456"
        assert expense_record.amount == -5000000  # -500 * 10000 multiplier
        assert expense_record.price == Decimal("0.001")
        assert expense_record.tx_hash == "node_67890"  # node_id mapped to tx_hash
        assert expense_record.token is None
        assert expense_record.chain is None
        assert expense_record.provider == "ALEPH"
        assert expense_record.origin == "exec_12345"  # execution_id mapped to origin
        assert expense_record.origin_ref == "expense_ref"
        assert expense_record.payment_method == "credit_expense"
        assert expense_record.credit_ref == "expense_msg_with_fields_789"
        assert expense_record.credit_index == 0
        assert expense_record.expiration_date is None
        assert expense_record.message_timestamp == message_timestamp


def test_update_credit_balances_transfer(session_factory: DbSessionFactory):
    """Test direct database insertion for credit transfer messages."""
    # Far-future expiration so this test never breaks due to real-world clock drift
    future_expiration_dt = dt.datetime(2100, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
    future_expiration_ms = int(future_expiration_dt.timestamp() * 1000)

    dist_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    message_timestamp = dt.datetime(2023, 1, 3, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        # Give sender enough credits with the same expiration as the transfer request
        update_credit_balances_distribution(
            session=session,
            credits_list=[
                {
                    "address": "0xsender",
                    "amount": 300,
                    "price": "1.0",
                    "tx_hash": "0xdist",
                    "provider": "test_provider",
                    "expiration": future_expiration_ms,
                }
            ],
            token="ALEPH",
            chain="ETH",
            message_hash="dist_for_transfer_test",
            message_timestamp=dist_timestamp,
        )

        update_credit_balances_transfer(
            session=session,
            credits_list=[
                {"address": "0x789", "amount": 250, "expiration": future_expiration_ms}
            ],
            sender_address="0xsender",
            whitelisted_addresses=["0xwhitelisted"],
            message_hash="transfer_msg_456",
            message_timestamp=message_timestamp,
        )
        session.commit()

        # Verify both sender and recipient records were created
        records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="transfer_msg_456")
            .all()
        )

        assert (
            len(records) == 2
        )  # One for sender (negative) and one for recipient (positive)

        # Find recipient record (positive amount)
        recipient_record = next(
            r for r in records if r.amount == 2500000
        )  # 250 * 10000
        assert recipient_record.address == "0x789"
        assert recipient_record.amount == 2500000  # 250 * 10000 multiplier
        assert recipient_record.expiration_date == future_expiration_dt
        assert recipient_record.provider == "ALEPH"
        assert recipient_record.payment_method == "credit_transfer"
        assert recipient_record.origin == "0xsender"
        assert recipient_record.price is None
        assert recipient_record.tx_hash is None
        assert recipient_record.token is None
        assert recipient_record.chain is None
        assert recipient_record.origin_ref is None
        assert recipient_record.credit_ref == "transfer_msg_456"
        assert recipient_record.credit_index == 0

        # Find sender record (negative amount)
        sender_record = next(r for r in records if r.amount == -2500000)  # -250 * 10000
        assert sender_record.address == "0xsender"
        assert sender_record.amount == -2500000  # -250 * 10000 multiplier
        assert sender_record.expiration_date is None
        assert sender_record.provider == "ALEPH"
        assert sender_record.payment_method == "credit_transfer"
        assert sender_record.origin == "0x789"
        assert sender_record.price is None
        assert sender_record.tx_hash is None
        assert sender_record.token is None
        assert sender_record.chain is None
        assert sender_record.origin_ref is None
        assert sender_record.credit_ref == "transfer_msg_456"
        assert sender_record.credit_index == 1


def test_whitelisted_sender_transfer(session_factory: DbSessionFactory):
    """Test transfer from whitelisted address - only recipient gets credits."""
    credits_list = [
        {
            "address": "0xrecipient",
            "amount": 500,
            "expiration": 1700000000000,
        }
    ]

    message_timestamp = dt.datetime(2023, 1, 4, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xwhitelisted",  # This sender is in the whitelist
            whitelisted_addresses=["0xwhitelisted", "0xother_whitelist"],
            message_hash="whitelist_transfer_123",
            message_timestamp=message_timestamp,
        )
        session.commit()

        # Verify only recipient record was created (no negative entry for whitelisted sender)
        records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="whitelist_transfer_123")
            .all()
        )

        assert len(records) == 1  # Only recipient record, no sender debit

        recipient_record = records[0]
        assert recipient_record.address == "0xrecipient"
        assert recipient_record.amount == 5000000  # 500 * 10000 multiplier
        assert recipient_record.origin == "0xwhitelisted"
        assert recipient_record.provider == "ALEPH"
        assert recipient_record.payment_method == "credit_transfer"
        assert recipient_record.credit_index == 0


def test_balance_validation_insufficient_credits(session_factory: DbSessionFactory):
    """Test balance validation fails when sender has insufficient credits."""

    # Create initial balance of 500
    credits_list = [
        {
            "address": "0xlow_balance",
            "amount": 500,
            "price": "1.0",
            "tx_hash": "0xinit",
            "provider": "test_provider",
            "expiration": 2000000000000,
        }
    ]

    message_timestamp = dt.datetime(2023, 1, 5, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=credits_list,
            token="TEST",
            chain="ETH",
            message_hash="low_balance_init",
            message_timestamp=message_timestamp,
        )
        session.commit()

        # Should pass for amounts <= 5000000 (500 * 10000 multiplier)
        assert validate_credit_transfer_balance(session, "0xlow_balance", 5000000)
        assert validate_credit_transfer_balance(session, "0xlow_balance", 4000000)

        # Should fail for amounts > 5000000
        assert not validate_credit_transfer_balance(session, "0xlow_balance", 6000000)
        assert not validate_credit_transfer_balance(session, "0xlow_balance", 10000000)


def test_expired_credits_excluded_from_transfers(session_factory: DbSessionFactory):
    """Test that expired credits are not counted when validating transfers."""

    expired_timestamp = int((time.time() - 86400) * 1000)  # 1 day ago
    valid_timestamp = int((time.time() + 86400) * 1000)  # 1 day from now

    credits_list = [
        {
            "address": "0xexpired_user",
            "amount": 800,  # Expired credits
            "price": "1.0",
            "tx_hash": "0xexpired",
            "provider": "test_provider",
            "expiration": expired_timestamp,
        },
        {
            "address": "0xexpired_user",
            "amount": 200,  # Valid credits
            "price": "1.0",
            "tx_hash": "0xvalid",
            "provider": "test_provider",
            "expiration": valid_timestamp,
        },
    ]

    with session_factory() as session:
        message_timestamp_1 = dt.datetime(2023, 1, 6, 12, 0, 0, tzinfo=dt.timezone.utc)

        update_credit_balances_distribution(
            session=session,
            credits_list=credits_list,
            token="TEST",
            chain="ETH",
            message_hash="expired_credits_test",
            message_timestamp=message_timestamp_1,
        )
        session.commit()

        # Total balance should only be 2000000 (200 * 10000, expired credits excluded)
        balance = get_credit_balance(session, "0xexpired_user")
        assert balance == 2000000

        # Transfer validation should only consider valid credits (2000000)

        assert validate_credit_transfer_balance(session, "0xexpired_user", 2000000)
        assert not validate_credit_transfer_balance(session, "0xexpired_user", 3000000)


def test_multiple_recipients_single_transfer(session_factory: DbSessionFactory):
    """Test transfer to multiple recipients in one transaction."""
    credits_list = [
        {
            "address": "0xrecipient1",
            "amount": 300,
            "expiration": 1700000000000,
        },
        {
            "address": "0xrecipient2",
            "amount": 200,
        },
        {
            "address": "0xrecipient3",
            "amount": 150,
            "expiration": 1800000000000,
        },
    ]

    with session_factory() as session:
        message_timestamp = dt.datetime(2023, 1, 7, 12, 0, 0, tzinfo=dt.timezone.utc)

        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xmulti_sender",
            whitelisted_addresses=[],
            message_hash="multi_recipient_transfer",
            message_timestamp=message_timestamp,
        )
        session.commit()

        records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="multi_recipient_transfer")
            .all()
        )

        # Should have 6 records: 3 positive (recipients) + 3 negative (sender)
        assert len(records) == 6

        # Check all positive records (recipients)
        positive_records = [r for r in records if r.amount > 0]
        assert len(positive_records) == 3

        amounts = {r.address: r.amount for r in positive_records}
        assert amounts["0xrecipient1"] == 3000000  # 300 * 10000 multiplier
        assert amounts["0xrecipient2"] == 2000000  # 200 * 10000 multiplier
        assert amounts["0xrecipient3"] == 1500000  # 150 * 10000 multiplier

        # Check all negative records (sender debits)
        negative_records = [r for r in records if r.amount < 0]
        assert len(negative_records) == 3

        for neg_record in negative_records:
            assert neg_record.address == "0xmulti_sender"
            assert neg_record.amount in [
                -3000000,
                -2000000,
                -1500000,
            ]  # Multiplied by 10000


def test_zero_amount_edge_case(session_factory: DbSessionFactory):
    """Test handling of zero-amount transfers."""
    credits_list = [
        {
            "address": "0xzero_recipient",
            "amount": 0,
            "expiration": 1700000000000,
        }
    ]

    with session_factory() as session:
        message_timestamp = dt.datetime(2023, 1, 8, 12, 0, 0, tzinfo=dt.timezone.utc)

        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xzero_sender",
            whitelisted_addresses=[],
            message_hash="zero_amount_transfer",
            message_timestamp=message_timestamp,
        )
        session.commit()

        records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="zero_amount_transfer")
            .all()
        )

        # Should still create both records
        assert len(records) == 2

        # Both should have zero amounts
        for record in records:
            assert record.amount == 0

        # Verify addresses
        addresses = {r.address for r in records}
        assert "0xzero_recipient" in addresses
        assert "0xzero_sender" in addresses


def test_self_transfer_edge_case(session_factory: DbSessionFactory):
    """Test transfer where sender and recipient are the same address."""
    credits_list = [
        {
            "address": "0xself_transfer_user",
            "amount": 250,
            "expiration": 1700000000000,
        }
    ]

    with session_factory() as session:
        message_timestamp = dt.datetime(2023, 1, 9, 12, 0, 0, tzinfo=dt.timezone.utc)

        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xself_transfer_user",  # Same as recipient
            whitelisted_addresses=[],
            message_hash="self_transfer_test",
            message_timestamp=message_timestamp,
        )
        session.commit()

        records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="self_transfer_test")
            .all()
        )

        # Should create both positive and negative records
        assert len(records) == 2

        amounts = [r.amount for r in records]
        assert 2500000 in amounts  # Positive entry (250 * 10000 multiplier)
        assert -2500000 in amounts  # Negative entry (-250 * 10000 multiplier)

        # Both records should have same address
        for record in records:
            assert record.address == "0xself_transfer_user"


def test_balance_fix_doesnt_affect_valid_credits(session_factory: DbSessionFactory):
    """Test that the negative balance fix doesn't affect normal scenarios."""

    # Create valid credits (far future expiration)
    valid_timestamp = int((time.time() + 86400 * 365) * 1000)  # 1 year from now

    credits_list = [
        {
            "address": "0xvalid_user",
            "amount": 1000,
            "price": "1.0",
            "tx_hash": "0xvalid",
            "provider": "test_provider",
            "expiration": valid_timestamp,
        }
    ]

    message_timestamp_1 = dt.datetime(2023, 1, 11, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=credits_list,
            token="TEST",
            chain="ETH",
            message_hash="valid_credits_msg",
            message_timestamp=message_timestamp_1,
        )
        session.commit()

        # Balance should be 10000000 (1000 * 10000 multiplier)
        balance = get_credit_balance(session, "0xvalid_user")
        assert balance == 10000000

        # Add some expenses
        expense_credits = [
            {
                "address": "0xvalid_user",
                "amount": 300,
                "ref": "expense_ref",
            }
        ]

        expense_timestamp = dt.datetime(2023, 1, 11, 13, 0, 0, tzinfo=dt.timezone.utc)

        update_credit_balances_expense(
            session=session,
            credits_list=expense_credits,
            message_hash="valid_expense_msg",
            message_timestamp=expense_timestamp,
        )
        session.commit()

        # Balance should be 7000000 (700 * 10000 multiplier: 10000000 - 3000000)
        balance = get_credit_balance(session, "0xvalid_user")
        assert balance == 7000000

        # Add a transfer (user sends 200 to someone else)
        transfer_credits = [
            {
                "address": "0xother_user",
                "amount": 200,
                "expiration": valid_timestamp,
            }
        ]

        transfer_timestamp = dt.datetime(2023, 1, 11, 14, 0, 0, tzinfo=dt.timezone.utc)

        update_credit_balances_transfer(
            session=session,
            credits_list=transfer_credits,
            sender_address="0xvalid_user",
            whitelisted_addresses=[],
            message_hash="valid_transfer_msg",
            message_timestamp=transfer_timestamp,
        )
        session.commit()

        # Balance should be 5000000 (500 * 10000 multiplier: 7000000 - 2000000)
        balance = get_credit_balance(session, "0xvalid_user")
        assert balance == 5000000

        # Verify the fix doesn't interfere with normal positive balances
        assert balance > 0


def test_fifo_scenario_1_non_expiring_first_equals_0_remaining(
    session_factory: DbSessionFactory,
):
    """
    FIFO Scenario 1: Non-expiring credits received FIRST → Result: 0 remaining

    Setup:
    - 1000 non-expiring credits (received FIRST at T1)
    - 1000 expiring credits (received SECOND at T2, expire at T4)
    - 1500 expense at T3 (before expiration at T4)

    FIFO Consumption: 1000 (non-expiring) + 500 (expiring) = 1500 total consumed
    Final Balance: 0 (non-expiring remaining) + 500 (expiring remaining but expired) = 0
    """

    # Use fixed timestamps before the credit precision cutoff (2026-02-02)
    # to ensure the 10000x multiplier is applied consistently
    # Base time: 2023-06-15 12:00:00 UTC
    base_time = 1686830400

    # Expiration: 2023-06-15 11:55:00 UTC (5 minutes before base_time, so expired)
    expiration_time = int((base_time - 300) * 1000)

    message_timestamp_1 = dt.datetime.fromtimestamp(
        base_time - 3600, tz=dt.timezone.utc
    )  # Non-expiring credits (FIRST)
    message_timestamp_2 = dt.datetime.fromtimestamp(
        base_time - 1800, tz=dt.timezone.utc
    )  # Expiring credits (SECOND)
    expense_timestamp = dt.datetime.fromtimestamp(
        base_time - 600, tz=dt.timezone.utc
    )  # Expense (BEFORE expiration at -300)

    # The "now" time for balance calculation (after expiration)
    now_time = dt.datetime.fromtimestamp(base_time, tz=dt.timezone.utc)

    with session_factory() as session:
        # Add 1000 non-expiring credits (FIRST chronologically)
        credits_no_expiry = [
            {
                "address": "0xcorner_case_user",
                "amount": 1000,
                "price": "1.0",
                "tx_hash": "0xno_expiry",
                "provider": "test_provider",
            }
        ]

        update_credit_balances_distribution(
            session=session,
            credits_list=credits_no_expiry,
            token="TEST",
            chain="ETH",
            message_hash="no_expiry_credits",
            message_timestamp=message_timestamp_1,
        )
        session.commit()

        # Add 1000 expiring credits (SECOND chronologically)
        credits_with_expiry = [
            {
                "address": "0xcorner_case_user",
                "amount": 1000,
                "price": "1.0",
                "tx_hash": "0xwith_expiry",
                "provider": "test_provider",
                "expiration": expiration_time,
            }
        ]

        update_credit_balances_distribution(
            session=session,
            credits_list=credits_with_expiry,
            token="TEST",
            chain="ETH",
            message_hash="expiring_credits",
            message_timestamp=message_timestamp_2,
        )
        session.commit()

        # Step 3: Add expense of 1500 BEFORE expiration
        expense_credits = [
            {
                "address": "0xcorner_case_user",
                "amount": 1500,
                "ref": "big_expense",
            }
        ]

        update_credit_balances_expense(
            session=session,
            credits_list=expense_credits,
            message_hash="big_expense_msg",
            message_timestamp=expense_timestamp,  # This is BEFORE tomorrow's expiration
        )
        session.commit()

        # Check final balance (expiring credits have already expired)
        # Pass the fixed "now" time so the test is deterministic
        balance_after_expiration = get_credit_balance(
            session, "0xcorner_case_user", now_time
        )

        # Expected: 0 remaining (all non-expiring consumed, expiring remainder expired)
        expected_balance = 0
        assert (
            balance_after_expiration == expected_balance
        ), f"Scenario 1: Expected {expected_balance} remaining credits, got {balance_after_expiration}"


def test_fifo_scenario_2_expiring_first_equals_500_remaining(
    session_factory: DbSessionFactory,
):
    """
    FIFO Scenario 2: Expiring credits received FIRST → Result: 500 remaining

    Setup:
    - 1000 expiring credits (received FIRST at T1, expire at T4)
    - 1000 non-expiring credits (received SECOND at T2)
    - 1500 expense at T3 (before expiration at T4)

    FIFO Consumption: 1000 (expiring) + 500 (non-expiring) = 1500 total consumed
    Final Balance: 0 (expiring remaining but expired) + 500 (non-expiring remaining) = 500
    """

    # Use fixed timestamps before the credit precision cutoff (2026-02-02)
    # to ensure the 10000x multiplier is applied consistently
    # Base time: 2023-06-15 12:00:00 UTC
    base_time = 1686830400

    # Expiration: 2023-06-15 11:55:00 UTC (5 minutes before base_time, so expired)
    expiration_time = int((base_time - 300) * 1000)

    message_timestamp_1 = dt.datetime.fromtimestamp(
        base_time - 3600, tz=dt.timezone.utc
    )  # Expiring credits (FIRST)
    message_timestamp_2 = dt.datetime.fromtimestamp(
        base_time - 1800, tz=dt.timezone.utc
    )  # Non-expiring credits (SECOND)
    expense_timestamp = dt.datetime.fromtimestamp(
        base_time - 600, tz=dt.timezone.utc
    )  # Expense (BEFORE expiration at -300)

    # The "now" time for balance calculation (after expiration)
    now_time = dt.datetime.fromtimestamp(base_time, tz=dt.timezone.utc)

    with session_factory() as session:
        # Add 1000 expiring credits (FIRST chronologically)
        credits_with_expiry = [
            {
                "address": "0xscenario2_user",
                "amount": 1000,
                "price": "1.0",
                "tx_hash": "0xexpiry_first",
                "provider": "test_provider",
                "expiration": expiration_time,
            }
        ]

        update_credit_balances_distribution(
            session=session,
            credits_list=credits_with_expiry,
            token="TEST",
            chain="ETH",
            message_hash="expiring_credits_first",
            message_timestamp=message_timestamp_1,  # FIRST timestamp
        )
        session.commit()

        # Add 1000 non-expiring credits (SECOND chronologically)
        credits_no_expiry = [
            {
                "address": "0xscenario2_user",
                "amount": 1000,
                "price": "1.0",
                "tx_hash": "0xno_expiry_second",
                "provider": "test_provider",
            }
        ]

        update_credit_balances_distribution(
            session=session,
            credits_list=credits_no_expiry,
            token="TEST",
            chain="ETH",
            message_hash="no_expiry_credits_second",
            message_timestamp=message_timestamp_2,  # SECOND timestamp
        )
        session.commit()

        # Step 3: Add expense of 1500 BEFORE expiration
        expense_credits = [
            {
                "address": "0xscenario2_user",
                "amount": 1500,
                "ref": "big_expense_scenario2",
            }
        ]

        update_credit_balances_expense(
            session=session,
            credits_list=expense_credits,
            message_hash="big_expense_msg_scenario2",
            message_timestamp=expense_timestamp,  # This is BEFORE tomorrow's expiration
        )
        session.commit()

        # Check final balance (expiring credits have already expired)
        # Pass the fixed "now" time so the test is deterministic
        balance_after_expiration = get_credit_balance(
            session, "0xscenario2_user", now_time
        )

        # Expected: 5000000 remaining (500 * 10000 multiplier - expiring consumed and expired, non-expiring remainder survives)
        expected_balance = 5000000
        assert (
            balance_after_expiration == expected_balance
        ), f"Scenario 2: Expected {expected_balance} remaining credits, got {balance_after_expiration}"


def test_cache_invalidation_on_credit_expiration(session_factory: DbSessionFactory):
    """
    Test that cached balances are recalculated when credits expire.

    This test covers the bug where cached balances were not being recalculated
    when credits expired after the cache was last updated.

    Bug scenario:
    1. Credit with expiration date X is added at time T1
    2. Cache is calculated at time T2 (where T1 < T2 < X)
    3. Current time is T3 (where T2 < X < T3, so credit has expired)
    4. Without the fix, cached balance would be returned incorrectly
    5. With the fix, balance is recalculated because credit expired after cache update
    """

    # Use fixed timestamps before the credit precision cutoff (2026-02-02)
    # to ensure the 10000x multiplier is applied consistently
    # Base time (T3): 2023-06-15 12:00:00 UTC
    base_time = 1686830400

    # Time T1: Add credit (1 hour before base_time)
    credit_time = dt.datetime.fromtimestamp(base_time - 3600, tz=dt.timezone.utc)

    # Time T2: Cache calculation time (30 minutes before base_time, before expiration)
    cache_time = dt.datetime.fromtimestamp(base_time - 1800, tz=dt.timezone.utc)

    # Time X: Credit expiration (5 minutes before base_time, between cache time and T3)
    expiration_time = int((base_time - 300) * 1000)

    # Time T3: Current time for final balance check (after expiration)
    now_time = dt.datetime.fromtimestamp(base_time, tz=dt.timezone.utc)

    with session_factory() as session:
        # Step 1: Add credit with expiration date
        credits_list = [
            {
                "address": "0xcache_bug_user",
                "amount": 1000,
                "price": "1.0",
                "tx_hash": "0xcache_test",
                "provider": "test_provider",
                "expiration": expiration_time,  # Will expire at T3
            }
        ]

        update_credit_balances_distribution(
            session=session,
            credits_list=credits_list,
            token="TEST",
            chain="ETH",
            message_hash="cache_expiration_test",
            message_timestamp=credit_time,  # T1
        )
        session.commit()

        # Step 2: Simulate cache being calculated at T2 (before expiration)
        # Mock utc_now to return cache_time during first balance calculation
        balance_before_expiration = get_credit_balance(
            session, "0xcache_bug_user", cache_time
        )
        session.commit()

        # Verify that at T2, the balance was 10000000 (1000 * 10000 multiplier, credit not yet expired)
        assert balance_before_expiration == 10000000

        # Verify that a cache entry was created and manually update its timestamp
        # to simulate it being created at T2 (cache_time)

        cached_balance = session.execute(
            select(AlephCreditBalanceDb).where(
                AlephCreditBalanceDb.address == "0xcache_bug_user"
            )
        ).scalar_one_or_none()

        assert cached_balance is not None
        assert cached_balance.balance == 10000000
        assert cached_balance.last_update == cache_time

        # Step 3: Now check balance at current time (T3, after expiration)
        # The fix should detect that credit expired after cache update and recalculate
        balance_after_expiration = get_credit_balance(
            session, "0xcache_bug_user", now_time
        )

        # Expected: 0 (credit has expired)
        assert balance_after_expiration == 0

        # Verify that cache was updated (should have a newer timestamp)
        session.refresh(cached_balance)
        assert cached_balance.balance == 0
        assert cached_balance.last_update == now_time


def test_get_resource_consumed_credits_no_records(session_factory: DbSessionFactory):
    """Test get_resource_consumed_credits returns 0 when no records exist."""

    with session_factory() as session:
        consumed_credits = get_resource_consumed_credits(
            session=session, item_hash="nonexistent_hash"
        )
        assert consumed_credits == 0


def test_get_resource_consumed_credits_single_record(session_factory: DbSessionFactory):
    """Test get_resource_consumed_credits with a single expense record."""

    # Create a credit expense record
    expense_credits = [
        {
            "address": "0xtest_user",
            "amount": 150,
            "ref": "resource_123",
        }
    ]

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        # Add the expense record with origin set to the resource hash
        update_credit_balances_expense(
            session=session,
            credits_list=expense_credits,
            message_hash="expense_msg_123",
            message_timestamp=message_timestamp,
        )

        # Manually set the origin field to the item_hash we want to test
        # Since update_credit_balances_expense doesn't set origin by default

        session.execute(
            sql_update(AlephCreditHistoryDb)
            .where(AlephCreditHistoryDb.credit_ref == "expense_msg_123")
            .values(origin="resource_123")
        )
        session.commit()

        consumed_credits = get_resource_consumed_credits(
            session=session, item_hash="resource_123"
        )
        assert consumed_credits == 1500000  # 150 * 10000 multiplier


def test_get_resource_consumed_credits_multiple_records(
    session_factory: DbSessionFactory,
):
    """Test get_resource_consumed_credits with multiple expense records for the same resource."""

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        # Create multiple expense records for the same resource
        expense_batches: List[Dict[str, Any]] = [
            {
                "credits": [
                    {"address": "0xuser1", "amount": 100, "ref": "resource_456"}
                ],
                "message_hash": "expense_msg_1",
            },
            {
                "credits": [
                    {"address": "0xuser2", "amount": 250, "ref": "resource_456"}
                ],
                "message_hash": "expense_msg_2",
            },
            {
                "credits": [
                    {"address": "0xuser3", "amount": 75, "ref": "resource_456"}
                ],
                "message_hash": "expense_msg_3",
            },
        ]

        # Import required modules

        for batch in expense_batches:
            credits_list: List[Dict[str, Any]] = batch["credits"]
            message_hash: str = batch["message_hash"]
            update_credit_balances_expense(
                session=session,
                credits_list=credits_list,
                message_hash=message_hash,
                message_timestamp=message_timestamp,
            )

        # Set origin for all records

        for batch in expense_batches:
            batch_message_hash = batch["message_hash"]
            session.execute(
                sql_update(AlephCreditHistoryDb)
                .where(AlephCreditHistoryDb.credit_ref == batch_message_hash)
                .values(origin="resource_456")
            )
        session.commit()

        consumed_credits = get_resource_consumed_credits(
            session=session, item_hash="resource_456"
        )
        # Total: (100 + 250 + 75) * 10000 = 4250000
        assert consumed_credits == 4250000


def test_get_resource_consumed_credits_filters_by_payment_method(
    session_factory: DbSessionFactory,
):
    """Test that get_resource_consumed_credits only counts credit_expense payments."""

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        # Add credit distribution (should be ignored)
        distribution_credits = [
            {
                "address": "0xuser1",
                "amount": 500,
                "price": "1.0",
                "tx_hash": "0xdist",
                "provider": "test_provider",
                "expiration": 2000000000000,
            }
        ]
        update_credit_balances_distribution(
            session=session,
            credits_list=distribution_credits,
            token="TEST",
            chain="ETH",
            message_hash="distribution_msg",
            message_timestamp=message_timestamp,
        )

        # Add credit transfer (should be ignored)
        transfer_credits = [{"address": "0xuser2", "amount": 200}]
        update_credit_balances_transfer(
            session=session,
            credits_list=transfer_credits,
            sender_address="0xsender",
            whitelisted_addresses=[],
            message_hash="transfer_msg",
            message_timestamp=message_timestamp,
        )

        # Add credit expense (should be counted)
        expense_credits = [{"address": "0xuser3", "amount": 150, "ref": "resource_789"}]
        update_credit_balances_expense(
            session=session,
            credits_list=expense_credits,
            message_hash="expense_msg",
            message_timestamp=message_timestamp,
        )

        # Set origin for all records to the same resource

        for msg_hash in ["distribution_msg", "transfer_msg", "expense_msg"]:
            session.execute(
                sql_update(AlephCreditHistoryDb)
                .where(AlephCreditHistoryDb.credit_ref == msg_hash)
                .values(origin="resource_789")
            )
        session.commit()

        consumed_credits = get_resource_consumed_credits(
            session=session, item_hash="resource_789"
        )
        # Only the expense (150 * 10000) should be counted, not distribution or transfer
        assert consumed_credits == 1500000


def test_get_resource_consumed_credits_filters_by_origin(
    session_factory: DbSessionFactory,
):
    """Test that get_resource_consumed_credits only counts records with matching origin."""

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        # Create expense records for different resources
        expenses: List[Dict[str, Any]] = [
            {
                "credits": [{"address": "0xuser1", "amount": 100}],
                "message_hash": "expense_resource_a",
                "origin": "resource_aaa",
            },
            {
                "credits": [{"address": "0xuser2", "amount": 200}],
                "message_hash": "expense_resource_b",
                "origin": "resource_bbb",
            },
            {
                "credits": [{"address": "0xuser3", "amount": 300}],
                "message_hash": "expense_resource_a_2",
                "origin": "resource_aaa",
            },
        ]

        # Import required modules

        for expense in expenses:
            credits_list: List[Dict[str, Any]] = expense["credits"]
            message_hash: str = expense["message_hash"]
            origin: str = expense["origin"]
            update_credit_balances_expense(
                session=session,
                credits_list=credits_list,
                message_hash=message_hash,
                message_timestamp=message_timestamp,
            )

            # Set the origin for this expense

            session.execute(
                sql_update(AlephCreditHistoryDb)
                .where(AlephCreditHistoryDb.credit_ref == message_hash)
                .values(origin=origin)
            )

        session.commit()

        # Test resource_aaa (should get 100 + 300 = 400, multiplied by 10000)
        consumed_credits_a = get_resource_consumed_credits(
            session=session, item_hash="resource_aaa"
        )
        assert consumed_credits_a == 4000000

        # Test resource_bbb (should get 200, multiplied by 10000)
        consumed_credits_b = get_resource_consumed_credits(
            session=session, item_hash="resource_bbb"
        )
        assert consumed_credits_b == 2000000

        # Test nonexistent resource (should get 0)
        consumed_credits_none = get_resource_consumed_credits(
            session=session, item_hash="resource_nonexistent"
        )
        assert consumed_credits_none == 0


def test_get_resource_consumed_credits_uses_absolute_values(
    session_factory: DbSessionFactory,
):
    """Test that get_resource_consumed_credits uses absolute values of amounts."""

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        # Create expense record
        expense_credits = [{"address": "0xuser", "amount": 250}]
        update_credit_balances_expense(
            session=session,
            credits_list=expense_credits,
            message_hash="expense_msg",
            message_timestamp=message_timestamp,
        )

        # Set origin

        session.execute(
            sql_update(AlephCreditHistoryDb)
            .where(AlephCreditHistoryDb.credit_ref == "expense_msg")
            .values(origin="resource_abs")
        )
        session.commit()

        # Verify that the expense record has negative amount (as expected from expense)
        expense_record = session.execute(
            select(AlephCreditHistoryDb).where(
                AlephCreditHistoryDb.credit_ref == "expense_msg"
            )
        ).scalar_one()
        assert (
            expense_record.amount == -2500000
        )  # Expenses are stored as negative (multiplied by 10000)

        # But get_resource_consumed_credits should return the absolute value
        consumed_credits = get_resource_consumed_credits(
            session=session, item_hash="resource_abs"
        )
        assert consumed_credits == 2500000


# ---------------------------------------------------------------------------
# Expiration propagation / re-transfer security tests
# ---------------------------------------------------------------------------

# Far-future timestamps so these tests never break due to real-world clock drift.
# Relationships: Z < X < Y
_EXP_X_DT = dt.datetime(2100, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
_EXP_X_MS = int(_EXP_X_DT.timestamp() * 1000)
_EXP_Y_DT = dt.datetime(2101, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)  # later than X
_EXP_Y_MS = int(_EXP_Y_DT.timestamp() * 1000)
_EXP_Z_DT = dt.datetime(2099, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)  # earlier than X
_EXP_Z_MS = int(_EXP_Z_DT.timestamp() * 1000)

_DIST_TS = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
_XFER_TS = dt.datetime(2023, 6, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


def _give_credits(
    session,
    address: str,
    amount: int,
    expiration_ms: int | None,
    msg_hash: str,
    msg_ts: dt.datetime,
) -> None:
    """Helper: distribute credits to an address."""
    entry: dict = {
        "address": address,
        "amount": amount,
        "price": "1.0",
        "tx_hash": "0xdist",
        "provider": "test_provider",
    }
    if expiration_ms is not None:
        entry["expiration"] = expiration_ms
    update_credit_balances_distribution(
        session=session,
        credits_list=[entry],
        token="ALEPH",
        chain="ETH",
        message_hash=msg_hash,
        message_timestamp=msg_ts,
    )


def test_transfer_expiration_propagated_to_recipient(
    session_factory: DbSessionFactory,
) -> None:
    """Re-transferring without expiration inherits the source's expiration (X)."""
    with session_factory() as session:
        _give_credits(session, "0xB", 300, _EXP_X_MS, "dist_prop_1", _DIST_TS)

        update_credit_balances_transfer(
            session=session,
            credits_list=[{"address": "0xC", "amount": 200, "expiration": None}],
            sender_address="0xB",
            whitelisted_addresses=[],
            message_hash="xfer_prop_1",
            message_timestamp=_XFER_TS,
        )
        session.commit()

        recipient_records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="xfer_prop_1")
            .filter(AlephCreditHistoryDb.amount > 0)
            .all()
        )
        assert len(recipient_records) == 1
        assert recipient_records[0].expiration_date == _EXP_X_DT


def test_transfer_later_expiration_capped_to_source(
    session_factory: DbSessionFactory,
) -> None:
    """Re-transferring with a later expiration (Y > X) is capped to X."""
    with session_factory() as session:
        _give_credits(session, "0xB", 300, _EXP_X_MS, "dist_cap_1", _DIST_TS)

        update_credit_balances_transfer(
            session=session,
            credits_list=[{"address": "0xC", "amount": 200, "expiration": _EXP_Y_MS}],
            sender_address="0xB",
            whitelisted_addresses=[],
            message_hash="xfer_cap_1",
            message_timestamp=_XFER_TS,
        )
        session.commit()

        recipient_records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="xfer_cap_1")
            .filter(AlephCreditHistoryDb.amount > 0)
            .all()
        )
        assert len(recipient_records) == 1
        # Capped at source expiration X, not the requested Y
        assert recipient_records[0].expiration_date == _EXP_X_DT


def test_transfer_earlier_expiration_kept(
    session_factory: DbSessionFactory,
) -> None:
    """Re-transferring with an earlier expiration (Z < X) keeps Z."""
    with session_factory() as session:
        _give_credits(session, "0xB", 300, _EXP_X_MS, "dist_early_1", _DIST_TS)

        update_credit_balances_transfer(
            session=session,
            credits_list=[{"address": "0xC", "amount": 200, "expiration": _EXP_Z_MS}],
            sender_address="0xB",
            whitelisted_addresses=[],
            message_hash="xfer_early_1",
            message_timestamp=_XFER_TS,
        )
        session.commit()

        recipient_records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="xfer_early_1")
            .filter(AlephCreditHistoryDb.amount > 0)
            .all()
        )
        assert len(recipient_records) == 1
        # Z is more restrictive than X — keep Z
        assert recipient_records[0].expiration_date == _EXP_Z_DT


def test_mixed_credits_transfer_split_entries(
    session_factory: DbSessionFactory,
) -> None:
    """
    Sender has 100 expiring credits (oldest) + 200 non-expiring credits.
    Transferring 250 → FIFO consumes 100 expiring first, then 150 non-expiring, so
    recipient gets two entries: 100 expiring at X + 150 non-expiring.
    After X, recipient can only spend the 150 permanent credits.

    FIFO is intentionally used for both the transfer assignment and the balance
    calculation to keep accounting consistent and prevent expiring credits from
    being laundered into non-expiring ones via a transfer.
    """
    dist2_ts = dt.datetime(2023, 2, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        # Oldest batch: 100 credits expiring at X
        _give_credits(session, "0xB", 100, _EXP_X_MS, "dist_mixed_1", _DIST_TS)
        # Newer batch: 200 credits with no expiration
        _give_credits(session, "0xB", 200, None, "dist_mixed_2", dist2_ts)

        update_credit_balances_transfer(
            session=session,
            credits_list=[{"address": "0xC", "amount": 250, "expiration": None}],
            sender_address="0xB",
            whitelisted_addresses=[],
            message_hash="xfer_mixed_1",
            message_timestamp=_XFER_TS,
        )
        session.commit()

        recipient_records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="xfer_mixed_1")
            .filter(AlephCreditHistoryDb.amount > 0)
            .order_by(AlephCreditHistoryDb.credit_index)
            .all()
        )
        # Two entries: expiring slice first (FIFO), then permanent slice
        assert len(recipient_records) == 2

        expiring_entry = next(
            r for r in recipient_records if r.expiration_date is not None
        )
        permanent_entry = next(
            r for r in recipient_records if r.expiration_date is None
        )

        assert expiring_entry.amount == 1000000  # 100 * 10000 multiplier
        assert expiring_entry.expiration_date == _EXP_X_DT
        assert permanent_entry.amount == 1500000  # 150 * 10000 multiplier
        assert permanent_entry.expiration_date is None

        # After X expires, 0xC can only spend the 150 permanent credits
        balance_after_x = get_credit_balance(
            session, "0xC", now=_EXP_X_DT + dt.timedelta(days=1)
        )
        assert balance_after_x == 1500000  # 150 * 10000


def test_whitelisted_sender_expiration_not_constrained(
    session_factory: DbSessionFactory,
) -> None:
    """Whitelisted senders are not constrained — recipient keeps the requested expiration."""
    with session_factory() as session:
        update_credit_balances_transfer(
            session=session,
            credits_list=[{"address": "0xC", "amount": 100, "expiration": _EXP_Y_MS}],
            sender_address="0xwhitelisted",
            whitelisted_addresses=["0xwhitelisted"],
            message_hash="xfer_whitelist_1",
            message_timestamp=_XFER_TS,
        )
        session.commit()

        recipient_records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="xfer_whitelist_1")
            .filter(AlephCreditHistoryDb.amount > 0)
            .all()
        )
        assert len(recipient_records) == 1
        # No source-credit constraint for whitelisted senders: Y is kept as-is
        assert recipient_records[0].expiration_date == _EXP_Y_DT


def test_chain_transfer_a_b_c_expiration_and_balances(
    session_factory: DbSessionFactory,
) -> None:
    """
    Full A → B → C chain with progressive expiration reduction.

    Setup:
      A receives 100 non-expiring + 100 expiring at EXP_A (year 2400)

    Step 1 — A transfers 150 to B, capping expiration at EXP_B (year 2200 < 2400):
      FIFO consumes 100 expiring-at-EXP_A first, then 50 non-expiring.
      Both slices get effective_exp = min(EXP_A, EXP_B) = EXP_B and min(None, EXP_B) = EXP_B,
      so they merge into one entry: 150 credits expiring at EXP_B.

    Step 2 — B transfers 50 to C, capping expiration at EXP_C (year 2100 < 2200):
      B has 150 expiring at EXP_B; FIFO consumes 50 → C gets 50 expiring at EXP_C.

    Expected final balances (in multiplied units, 1 credit = 10_000 internal units):
      A: 50 non-expiring (always 500_000 regardless of time)
      B: 100 expiring at EXP_B (1_000_000 before EXP_B, 0 after)
      C: 50 expiring at EXP_C  (500_000 before EXP_C, 0 after)
    """
    # Far-future expirations that form a clear chain: EXP_C < EXP_B < EXP_A
    exp_a_dt = dt.datetime(2400, 1, 1, tzinfo=dt.timezone.utc)
    exp_a_ms = int(exp_a_dt.timestamp() * 1000)
    exp_b_dt = dt.datetime(2200, 1, 1, tzinfo=dt.timezone.utc)
    exp_b_ms = int(exp_b_dt.timestamp() * 1000)
    exp_c_dt = dt.datetime(2100, 1, 1, tzinfo=dt.timezone.utc)
    exp_c_ms = int(exp_c_dt.timestamp() * 1000)

    dist1_ts = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    dist2_ts = dt.datetime(2023, 2, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    xfer_ab_ts = dt.datetime(2023, 3, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    xfer_bc_ts = dt.datetime(2023, 4, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    MUL = 10_000  # precision multiplier for pre-cutoff messages

    with session_factory() as session:
        # --- Setup: give A two credit pools ---
        _give_credits(
            session, "0xA", 100, exp_a_ms, "dist_chain_1", dist1_ts
        )  # 100 expiring
        _give_credits(
            session, "0xA", 100, None, "dist_chain_2", dist2_ts
        )  # 100 permanent

        # --- Step 1: A transfers 150 to B, capping at EXP_B ---
        update_credit_balances_transfer(
            session=session,
            credits_list=[{"address": "0xB", "amount": 150, "expiration": exp_b_ms}],
            sender_address="0xA",
            whitelisted_addresses=[],
            message_hash="xfer_ab_chain",
            message_timestamp=xfer_ab_ts,
        )
        session.commit()

        # B should have one merged entry: 150 * MUL expiring at EXP_B
        b_records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="xfer_ab_chain")
            .filter(AlephCreditHistoryDb.amount > 0)
            .all()
        )
        assert len(b_records) == 1, "both slices share EXP_B so they must merge"
        assert b_records[0].amount == 150 * MUL
        assert b_records[0].expiration_date == exp_b_dt

        # --- Step 2: B transfers 50 to C, capping at EXP_C ---
        update_credit_balances_transfer(
            session=session,
            credits_list=[{"address": "0xC", "amount": 50, "expiration": exp_c_ms}],
            sender_address="0xB",
            whitelisted_addresses=[],
            message_hash="xfer_bc_chain",
            message_timestamp=xfer_bc_ts,
        )
        session.commit()

        # C should have one entry: 50 * MUL expiring at EXP_C
        c_records = (
            session.query(AlephCreditHistoryDb)
            .filter_by(credit_ref="xfer_bc_chain")
            .filter(AlephCreditHistoryDb.amount > 0)
            .all()
        )
        assert len(c_records) == 1
        assert c_records[0].amount == 50 * MUL
        assert c_records[0].expiration_date == exp_c_dt

        # --- Balance checks before any expiration ---
        now_before = dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc)
        assert get_credit_balance(session, "0xA", now=now_before) == 50 * MUL
        assert get_credit_balance(session, "0xB", now=now_before) == 100 * MUL
        assert get_credit_balance(session, "0xC", now=now_before) == 50 * MUL

        # --- After EXP_C: C's credits expire, A and B unchanged ---
        now_after_c = exp_c_dt + dt.timedelta(days=1)
        assert get_credit_balance(session, "0xA", now=now_after_c) == 50 * MUL
        assert get_credit_balance(session, "0xB", now=now_after_c) == 100 * MUL
        assert get_credit_balance(session, "0xC", now=now_after_c) == 0

        # --- After EXP_B: B's credits expire too, A still has permanent credits ---
        now_after_b = exp_b_dt + dt.timedelta(days=1)
        assert get_credit_balance(session, "0xA", now=now_after_b) == 50 * MUL
        assert get_credit_balance(session, "0xB", now=now_after_b) == 0
        assert get_credit_balance(session, "0xC", now=now_after_b) == 0

        # --- After EXP_A: all expiring pools gone, A's permanent slice survives ---
        now_after_a = exp_a_dt + dt.timedelta(days=1)
        assert get_credit_balance(session, "0xA", now=now_after_a) == 50 * MUL
        assert get_credit_balance(session, "0xB", now=now_after_a) == 0
        assert get_credit_balance(session, "0xC", now=now_after_a) == 0


# ── Credit balance details tests ──────────────────────────────────────


def _insert_credit_history_entries(session, entries: List[Dict[str, Any]]):
    """Helper to bulk-insert credit history rows for testing."""
    for entry in entries:
        session.add(AlephCreditHistoryDb(**entry))
    session.flush()


def test_credit_balance_details_non_expiring_only(session_factory: DbSessionFactory):
    """Details with only non-expiring credits."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xdetails1",
            "amount": 1000,
            "credit_ref": "d1_a",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": None,
        },
        {
            "address": "0xdetails1",
            "amount": 2000,
            "credit_ref": "d1_b",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=1),
            "last_update": ts,
            "expiration_date": None,
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        total, details = get_credit_balance_with_details(
            session=session, address="0xdetails1"
        )
        assert total == 3000
        assert len(details) == 1
        assert details[0].expiration_date is None
        assert details[0].amount == 3000


def test_credit_balance_details_mixed_expiration(session_factory: DbSessionFactory):
    """Details group by different expiration dates."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    exp1 = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
    exp2 = dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xdetails2",
            "amount": 1000,
            "credit_ref": "d2_a",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": None,
        },
        {
            "address": "0xdetails2",
            "amount": 500,
            "credit_ref": "d2_b",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=1),
            "last_update": ts,
            "expiration_date": exp1,
        },
        {
            "address": "0xdetails2",
            "amount": 300,
            "credit_ref": "d2_c",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=2),
            "last_update": ts,
            "expiration_date": exp2,
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        total, details = get_credit_balance_with_details(
            session=session, address="0xdetails2"
        )
        assert total == 1800
        # Non-expiring first, then by expiration_date ascending
        assert len(details) == 3
        assert details[0].expiration_date is None
        assert details[0].amount == 1000
        assert details[1].expiration_date == exp1
        assert details[1].amount == 500
        assert details[2].expiration_date == exp2
        assert details[2].amount == 300


def test_credit_balance_details_partial_consumption(session_factory: DbSessionFactory):
    """Details are accurate after FIFO partial consumption."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    exp1 = dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)

    entries = [
        # Non-expiring credit (oldest, consumed first by FIFO)
        {
            "address": "0xdetails3",
            "amount": 1000,
            "credit_ref": "d3_a",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": None,
        },
        # Expiring credit
        {
            "address": "0xdetails3",
            "amount": 500,
            "credit_ref": "d3_b",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=1),
            "last_update": ts,
            "expiration_date": exp1,
        },
        # Expense consuming 700 from the oldest (non-expiring) credit
        {
            "address": "0xdetails3",
            "amount": -700,
            "credit_ref": "d3_exp",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=2),
            "last_update": ts,
            "expiration_date": None,
            "payment_method": "credit_expense",
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        total, details = get_credit_balance_with_details(
            session=session, address="0xdetails3"
        )
        # 1000 - 700 = 300 non-expiring remaining, 500 expiring remaining
        assert total == 800
        assert len(details) == 2
        assert details[0].expiration_date is None
        assert details[0].amount == 300
        assert details[1].expiration_date == exp1
        assert details[1].amount == 500


def test_credit_balance_details_fully_consumed(session_factory: DbSessionFactory):
    """Fully consumed credits don't appear in details."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xdetails4",
            "amount": 500,
            "credit_ref": "d4_a",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": None,
        },
        {
            "address": "0xdetails4",
            "amount": -500,
            "credit_ref": "d4_exp",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=1),
            "last_update": ts,
            "expiration_date": None,
            "payment_method": "credit_expense",
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        total, details = get_credit_balance_with_details(
            session=session, address="0xdetails4"
        )
        assert total == 0
        assert len(details) == 0


def test_credit_balance_details_expired_excluded(session_factory: DbSessionFactory):
    """Expired credits are excluded from details."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    past_exp = dt.datetime(2026, 3, 15, tzinfo=dt.timezone.utc)
    future_exp = dt.datetime(2027, 6, 1, tzinfo=dt.timezone.utc)
    now = dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc)

    entries = [
        # Already expired
        {
            "address": "0xdetails5",
            "amount": 1000,
            "credit_ref": "d5_expired",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": past_exp,
        },
        # Still valid
        {
            "address": "0xdetails5",
            "amount": 500,
            "credit_ref": "d5_valid",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=1),
            "last_update": ts,
            "expiration_date": future_exp,
        },
        # Non-expiring
        {
            "address": "0xdetails5",
            "amount": 200,
            "credit_ref": "d5_noexp",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=2),
            "last_update": ts,
            "expiration_date": None,
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        total, details = get_credit_balance_with_details(
            session=session, address="0xdetails5", now=now
        )
        # Expired 1000 excluded, remaining: 500 + 200 = 700
        assert total == 700
        assert len(details) == 2
        assert details[0].expiration_date is None
        assert details[0].amount == 200
        assert details[1].expiration_date == future_exp
        assert details[1].amount == 500


def test_credit_balance_details_no_history(session_factory: DbSessionFactory):
    """No credit history returns 0 balance and empty details."""
    with session_factory() as session:
        total, details = get_credit_balance_with_details(
            session=session, address="0xno_history"
        )
        assert total == 0
        assert len(details) == 0


def test_credit_balance_details_matches_total(session_factory: DbSessionFactory):
    """Sum of details amounts equals the total balance."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    exp1 = dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)
    exp2 = dt.datetime(2026, 12, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xdetails6",
            "amount": 1000,
            "credit_ref": "d6_a",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": None,
        },
        {
            "address": "0xdetails6",
            "amount": 800,
            "credit_ref": "d6_b",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=1),
            "last_update": ts,
            "expiration_date": exp1,
        },
        {
            "address": "0xdetails6",
            "amount": 600,
            "credit_ref": "d6_c",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=2),
            "last_update": ts,
            "expiration_date": exp2,
        },
        # Expense consuming 1200 total (FIFO: 1000 from non-expiring, 200 from exp1)
        {
            "address": "0xdetails6",
            "amount": -1200,
            "credit_ref": "d6_exp",
            "credit_index": 0,
            "message_timestamp": ts + dt.timedelta(hours=3),
            "last_update": ts,
            "expiration_date": None,
            "payment_method": "credit_expense",
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        total, details = get_credit_balance_with_details(
            session=session, address="0xdetails6"
        )
        # 1000 - 1000 = 0 non-expiring, 800 - 200 = 600 exp1, 600 exp2
        assert total == 1200
        details_sum = sum(d.amount for d in details)
        assert details_sum == total

        assert len(details) == 2
        assert details[0].expiration_date == exp1
        assert details[0].amount == 600
        assert details[1].expiration_date == exp2
        assert details[1].amount == 600


# ── Volume (origin_ref) consumed credits tests ───────────────────────


def test_get_resource_consumed_credits_uses_origin_ref_for_volumes(
    session_factory: DbSessionFactory,
):
    """Volumes store the resource hash in origin_ref, not origin.
    get_resource_consumed_credits (via get_total_consumed_credits) must
    fall back to origin_ref when origin is empty."""

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {"address": "0xvol_user", "amount": 200, "ref": "vol_hash_1"}
            ],
            message_hash="vol_expense_1",
            message_timestamp=message_timestamp,
        )
        # Volume expenses: origin="" (no execution_id), origin_ref="vol_hash_1"
        session.commit()

        consumed = get_resource_consumed_credits(
            session=session, item_hash="vol_hash_1"
        )
        assert consumed == 2000000  # 200 * 10000


def test_get_total_consumed_credits_uses_origin_ref_for_volumes(
    session_factory: DbSessionFactory,
):
    """get_total_consumed_credits must match on origin_ref when origin is empty."""

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {"address": "0xvol_total", "amount": 100, "ref": "vol_total_hash"},
            ],
            message_hash="vol_total_expense",
            message_timestamp=message_timestamp,
        )
        session.commit()

        # Filter by item_hash (should match via origin_ref)
        consumed = get_total_consumed_credits(
            session=session, item_hash="vol_total_hash"
        )
        assert consumed == 1000000  # 100 * 10000

        # Filter by address only (no origin check, should always work)
        consumed_addr = get_total_consumed_credits(
            session=session, address="0xvol_total"
        )
        assert consumed_addr == 1000000


def test_get_consumed_credits_by_resource_uses_origin_ref_for_volumes(
    session_factory: DbSessionFactory,
):
    """get_consumed_credits_by_resource must resolve origin_ref for volume
    resources whose origin is empty."""

    message_timestamp = dt.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        # Instance expense (has execution_id → origin is set)
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {
                    "address": "0xmixed",
                    "amount": 300,
                    "execution_id": "instance_hash_1",
                    "ref": "some_ref",
                },
            ],
            message_hash="inst_exp_mixed",
            message_timestamp=message_timestamp,
        )

        # Volume expense (no execution_id → origin is empty, ref → origin_ref)
        update_credit_balances_expense(
            session=session,
            credits_list=[
                {"address": "0xmixed", "amount": 150, "ref": "vol_hash_mixed"},
            ],
            message_hash="vol_exp_mixed",
            message_timestamp=message_timestamp,
        )
        session.commit()

        result = get_consumed_credits_by_resource(
            session=session,
            item_hashes=["instance_hash_1", "vol_hash_mixed"],
        )

        assert result["instance_hash_1"] == 3000000  # 300 * 10000
        assert result["vol_hash_mixed"] == 1500000  # 150 * 10000


# ── Credit history filter and sort tests ──────────────────────────────


def test_has_expiration_filter_true(session_factory: DbSessionFactory):
    """has_expiration=True returns only entries WITH an expiration_date."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    exp = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xfilter",
            "amount": 100,
            "credit_ref": "ref_exp",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": exp,
            "payment_method": "credit_distribution",
        },
        {
            "address": "0xfilter",
            "amount": 200,
            "credit_ref": "ref_noexp",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": None,
            "payment_method": "credit_distribution",
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        results = get_address_credit_history(
            session=session, address="0xfilter", has_expiration=True
        )
        assert len(results) == 1
        assert results[0].credit_ref == "ref_exp"


def test_has_expiration_filter_false(session_factory: DbSessionFactory):
    """has_expiration=False returns only entries WITHOUT an expiration_date."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    exp = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xfilter2",
            "amount": 100,
            "credit_ref": "ref2_exp",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": exp,
            "payment_method": "credit_distribution",
        },
        {
            "address": "0xfilter2",
            "amount": 200,
            "credit_ref": "ref2_noexp",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": None,
            "payment_method": "credit_distribution",
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        results = get_address_credit_history(
            session=session, address="0xfilter2", has_expiration=False
        )
        assert len(results) == 1
        assert results[0].credit_ref == "ref2_noexp"


def test_exclude_payment_method(session_factory: DbSessionFactory):
    """exclude_payment_method removes entries matching given payment methods."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xexclude",
            "amount": 100,
            "credit_ref": "ref_dist",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "payment_method": "credit_distribution",
        },
        {
            "address": "0xexclude",
            "amount": -50,
            "credit_ref": "ref_expense",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "payment_method": "credit_expense",
        },
        {
            "address": "0xexclude",
            "amount": 75,
            "credit_ref": "ref_transfer",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "payment_method": "credit_transfer",
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        # Exclude expense
        results = get_address_credit_history(
            session=session,
            address="0xexclude",
            exclude_payment_method=["credit_expense"],
        )
        assert len(results) == 2
        refs = {r.credit_ref for r in results}
        assert refs == {"ref_dist", "ref_transfer"}

        # Exclude both expense and transfer
        results = get_address_credit_history(
            session=session,
            address="0xexclude",
            exclude_payment_method=["credit_expense", "credit_transfer"],
        )
        assert len(results) == 1
        assert results[0].credit_ref == "ref_dist"


def test_exclude_payment_method_count(session_factory: DbSessionFactory):
    """count_address_credit_history respects exclude_payment_method."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xcnt",
            "amount": 100,
            "credit_ref": "cnt_dist",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "payment_method": "credit_distribution",
        },
        {
            "address": "0xcnt",
            "amount": -50,
            "credit_ref": "cnt_expense",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "payment_method": "credit_expense",
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        total = count_address_credit_history(session=session, address="0xcnt")
        assert total == 2

        filtered = count_address_credit_history(
            session=session,
            address="0xcnt",
            exclude_payment_method=["credit_expense"],
        )
        assert filtered == 1


def test_sort_by_amount_ascending(session_factory: DbSessionFactory):
    """sort_by=AMOUNT, sort_order=ASC orders by amount ascending."""
    base_ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xsort",
            "amount": 300,
            "credit_ref": "sort_a",
            "credit_index": 0,
            "message_timestamp": base_ts,
            "last_update": base_ts,
        },
        {
            "address": "0xsort",
            "amount": 100,
            "credit_ref": "sort_b",
            "credit_index": 0,
            "message_timestamp": base_ts + dt.timedelta(hours=1),
            "last_update": base_ts,
        },
        {
            "address": "0xsort",
            "amount": 200,
            "credit_ref": "sort_c",
            "credit_index": 0,
            "message_timestamp": base_ts + dt.timedelta(hours=2),
            "last_update": base_ts,
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        results = get_address_credit_history(
            session=session,
            address="0xsort",
            sort_by=SortByCreditHistory.AMOUNT,
            sort_order=SortOrder.ASCENDING,
        )
        amounts = [r.amount for r in results]
        assert amounts == [100, 200, 300]


def test_sort_by_amount_descending(session_factory: DbSessionFactory):
    """sort_by=AMOUNT, sort_order=DESC orders by amount descending."""
    base_ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xsortd",
            "amount": 300,
            "credit_ref": "sortd_a",
            "credit_index": 0,
            "message_timestamp": base_ts,
            "last_update": base_ts,
        },
        {
            "address": "0xsortd",
            "amount": 100,
            "credit_ref": "sortd_b",
            "credit_index": 0,
            "message_timestamp": base_ts + dt.timedelta(hours=1),
            "last_update": base_ts,
        },
        {
            "address": "0xsortd",
            "amount": 200,
            "credit_ref": "sortd_c",
            "credit_index": 0,
            "message_timestamp": base_ts + dt.timedelta(hours=2),
            "last_update": base_ts,
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        results = get_address_credit_history(
            session=session,
            address="0xsortd",
            sort_by=SortByCreditHistory.AMOUNT,
            sort_order=SortOrder.DESCENDING,
        )
        amounts = [r.amount for r in results]
        assert amounts == [300, 200, 100]


def test_sort_by_expiration_date_nulls_last(session_factory: DbSessionFactory):
    """Sorting by expiration_date puts NULLs last."""
    base_ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xnull",
            "amount": 100,
            "credit_ref": "null_a",
            "credit_index": 0,
            "message_timestamp": base_ts,
            "last_update": base_ts,
            "expiration_date": None,
        },
        {
            "address": "0xnull",
            "amount": 200,
            "credit_ref": "null_b",
            "credit_index": 0,
            "message_timestamp": base_ts + dt.timedelta(hours=1),
            "last_update": base_ts,
            "expiration_date": dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc),
        },
        {
            "address": "0xnull",
            "amount": 300,
            "credit_ref": "null_c",
            "credit_index": 0,
            "message_timestamp": base_ts + dt.timedelta(hours=2),
            "last_update": base_ts,
            "expiration_date": dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc),
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        # ASC: earliest expiration first, NULLs last
        results = get_address_credit_history(
            session=session,
            address="0xnull",
            sort_by=SortByCreditHistory.EXPIRATION_DATE,
            sort_order=SortOrder.ASCENDING,
        )
        refs = [r.credit_ref for r in results]
        assert refs == ["null_c", "null_b", "null_a"]

        # DESC: latest expiration first, NULLs last
        results = get_address_credit_history(
            session=session,
            address="0xnull",
            sort_by=SortByCreditHistory.EXPIRATION_DATE,
            sort_order=SortOrder.DESCENDING,
        )
        refs = [r.credit_ref for r in results]
        assert refs == ["null_b", "null_c", "null_a"]


def test_has_expiration_and_exclude_combined(session_factory: DbSessionFactory):
    """has_expiration and exclude_payment_method work together."""
    ts = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    exp = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)

    entries = [
        {
            "address": "0xcombo",
            "amount": 100,
            "credit_ref": "combo_a",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": exp,
            "payment_method": "credit_distribution",
        },
        {
            "address": "0xcombo",
            "amount": -50,
            "credit_ref": "combo_b",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": None,
            "payment_method": "credit_expense",
        },
        {
            "address": "0xcombo",
            "amount": 75,
            "credit_ref": "combo_c",
            "credit_index": 0,
            "message_timestamp": ts,
            "last_update": ts,
            "expiration_date": exp,
            "payment_method": "credit_expense",
        },
    ]

    with session_factory() as session:
        _insert_credit_history_entries(session, entries)
        session.commit()

        # has_expiration=True + exclude credit_expense => only combo_a
        results = get_address_credit_history(
            session=session,
            address="0xcombo",
            has_expiration=True,
            exclude_payment_method=["credit_expense"],
        )
        assert len(results) == 1
        assert results[0].credit_ref == "combo_a"
