import datetime as dt
import time
from decimal import Decimal
from typing import Any, Dict, List

from sqlalchemy import select
from sqlalchemy import update as sql_update

from aleph.db.accessors.balances import (
    get_credit_balance,
    get_resource_consumed_credits,
    update_credit_balances_distribution,
    update_credit_balances_expense,
    update_credit_balances_transfer,
    validate_credit_transfer_balance,
)
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb
from aleph.types.db_session import DbSessionFactory


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
    credits_list = [
        {
            "address": "0x789",  # recipient
            "amount": 250,
            "expiration": 1700000000000,  # timestamp in ms
        }
    ]

    message_timestamp = dt.datetime(2023, 1, 3, 12, 0, 0, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
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
        assert recipient_record.expiration_date is not None
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
    credit_time = dt.datetime.fromtimestamp(
        base_time - 3600, tz=dt.timezone.utc
    )

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
