import datetime as dt
from decimal import Decimal

import pytest

from aleph.db.accessors.balances import (
    get_credit_balance,
    update_credit_balances_distribution,
    update_credit_balances_expense,
    update_credit_balances_transfer,
)
from aleph.db.models import AlephCreditBalanceDb
from aleph.types.db_session import DbSessionFactory


def test_update_credit_balances_distribution(session_factory: DbSessionFactory):
    """Test direct database insertion for credit distribution messages."""
    credits_list = [
        {
            "address": "0x123",
            "amount": 1000,
            "ratio": "0.5",
            "tx_hash": "0xabc123",
            "provider": "test_provider",
            "expiration": 1700000000000,  # timestamp in ms
            "origin": "test_origin",
            "ref": "test_ref",
            "payment_method": "test_payment",
        }
    ]
    
    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=credits_list,
            token="TEST_TOKEN",
            chain="ETH",
            message_hash="msg_hash_123",
        )
        session.commit()
        
        # Verify credit balance was inserted
        credit_record = session.query(AlephCreditBalanceDb).filter_by(
            address="0x123", credit_ref="msg_hash_123"
        ).first()

        assert credit_record is not None
        assert credit_record.address == "0x123"
        assert credit_record.amount == 1000
        assert credit_record.ratio == Decimal("0.5")
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


def test_update_credit_balances_expense(session_factory: DbSessionFactory):
    """Test direct database insertion for credit expense messages."""
    credits_list = [
        {
            "address": "0x456",
            "amount": 500,
            "ref": "expense_ref",
        }
    ]
    
    with session_factory() as session:
        update_credit_balances_expense(
            session=session,
            credits_list=credits_list,
            message_hash="expense_msg_789",
        )
        session.commit()
        
        # Verify expense record was inserted
        expense_record = session.query(AlephCreditBalanceDb).filter_by(
            address="0x456", credit_ref="expense_msg_789"
        ).first()
        
        assert expense_record is not None
        assert expense_record.address == "0x456"
        assert expense_record.amount == -500
        assert expense_record.ratio is None
        assert expense_record.tx_hash is None
        assert expense_record.token is None
        assert expense_record.chain is None
        assert expense_record.provider == "ALEPH"
        assert expense_record.origin_ref == "expense_ref"
        assert expense_record.payment_method == "credit_expense"
        assert expense_record.credit_ref == "expense_msg_789"
        assert expense_record.credit_index == 0
        assert expense_record.expiration_date is None



def test_update_credit_balances_transfer(session_factory: DbSessionFactory):
    """Test direct database insertion for credit transfer messages."""
    credits_list = [
        {
            "address": "0x789",  # recipient
            "amount": 250,
            "expiration": 1700000000000,  # timestamp in ms
        }
    ]
    
    with session_factory() as session:
        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xsender",
            whitelisted_addresses=["0xwhitelisted"],
            message_hash="transfer_msg_456",
        )
        session.commit()
        
        # Verify both sender and recipient records were created
        records = session.query(AlephCreditBalanceDb).filter_by(
            credit_ref="transfer_msg_456"
        ).all()
        
        assert len(records) == 2  # One for sender (negative) and one for recipient (positive)
        
        # Find recipient record (positive amount)
        recipient_record = next(r for r in records if r.amount == 250)
        assert recipient_record.address == "0x789"
        assert recipient_record.amount == 250
        assert recipient_record.expiration_date is not None
        assert recipient_record.provider == "ALEPH"
        assert recipient_record.payment_method == "credit_transfer"
        assert recipient_record.origin == "0xsender"
        assert recipient_record.ratio is None
        assert recipient_record.tx_hash is None
        assert recipient_record.token is None
        assert recipient_record.chain is None
        assert recipient_record.origin_ref is None
        assert recipient_record.credit_ref == "transfer_msg_456"
        assert recipient_record.credit_index == 0

        
        # Find sender record (negative amount)
        sender_record = next(r for r in records if r.amount == -250)
        assert sender_record.address == "0xsender"
        assert sender_record.amount == -250
        assert sender_record.expiration_date is None
        assert sender_record.provider == "ALEPH"
        assert sender_record.payment_method == "credit_transfer"
        assert sender_record.origin == "0x789"
        assert sender_record.ratio is None
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
    
    with session_factory() as session:
        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xwhitelisted",  # This sender is in the whitelist
            whitelisted_addresses=["0xwhitelisted", "0xother_whitelist"],
            message_hash="whitelist_transfer_123",
        )
        session.commit()
        
        # Verify only recipient record was created (no negative entry for whitelisted sender)
        records = session.query(AlephCreditBalanceDb).filter_by(
            credit_ref="whitelist_transfer_123"
        ).all()
        
        assert len(records) == 1  # Only recipient record, no sender debit
        
        recipient_record = records[0]
        assert recipient_record.address == "0xrecipient"
        assert recipient_record.amount == 500
        assert recipient_record.origin == "0xwhitelisted"
        assert recipient_record.provider == "ALEPH"
        assert recipient_record.payment_method == "credit_transfer"
        assert recipient_record.credit_index == 0


def test_balance_validation_insufficient_credits(session_factory: DbSessionFactory):
    """Test balance validation fails when sender has insufficient credits."""
    from aleph.db.accessors.balances import validate_credit_transfer_balance
    
    # Create initial balance of 500
    credits_list = [
        {
            "address": "0xlow_balance",
            "amount": 500,
            "ratio": "1.0",
            "tx_hash": "0xinit",
            "provider": "test_provider",
            "expiration": 2000000000000,
        }
    ]
    
    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=credits_list,
            token="TEST",
            chain="ETH",
            message_hash="low_balance_init",
        )
        session.commit()
        
        # Should pass for amounts <= 500
        assert validate_credit_transfer_balance(session, "0xlow_balance", 500) == True
        assert validate_credit_transfer_balance(session, "0xlow_balance", 400) == True
        
        # Should fail for amounts > 500
        assert validate_credit_transfer_balance(session, "0xlow_balance", 600) == False
        assert validate_credit_transfer_balance(session, "0xlow_balance", 1000) == False


def test_expired_credits_excluded_from_transfers(session_factory: DbSessionFactory):
    """Test that expired credits are not counted when validating transfers."""
    import time
    
    expired_timestamp = int((time.time() - 86400) * 1000)  # 1 day ago
    valid_timestamp = int((time.time() + 86400) * 1000)    # 1 day from now
    
    credits_list = [
        {
            "address": "0xexpired_user",
            "amount": 800,  # Expired credits
            "ratio": "1.0",
            "tx_hash": "0xexpired",
            "provider": "test_provider", 
            "expiration": expired_timestamp,
        },
        {
            "address": "0xexpired_user",
            "amount": 200,  # Valid credits
            "ratio": "1.0",
            "tx_hash": "0xvalid",
            "provider": "test_provider",
            "expiration": valid_timestamp,
        }
    ]
    
    with session_factory() as session:
        update_credit_balances_distribution(
            session=session,
            credits_list=credits_list,
            token="TEST",
            chain="ETH",
            message_hash="expired_credits_test",
        )
        session.commit()
        
        # Total balance should only be 200 (expired credits excluded)
        balance = get_credit_balance(session, "0xexpired_user")
        assert balance == 200
        
        # Transfer validation should only consider valid credits (200)
        from aleph.db.accessors.balances import validate_credit_transfer_balance
        assert validate_credit_transfer_balance(session, "0xexpired_user", 200) == True
        assert validate_credit_transfer_balance(session, "0xexpired_user", 300) == False


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
        }
    ]
    
    with session_factory() as session:
        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xmulti_sender",
            whitelisted_addresses=[],
            message_hash="multi_recipient_transfer",
        )
        session.commit()
        
        records = session.query(AlephCreditBalanceDb).filter_by(
            credit_ref="multi_recipient_transfer"
        ).all()
        
        # Should have 6 records: 3 positive (recipients) + 3 negative (sender)
        assert len(records) == 6
        
        # Check all positive records (recipients)
        positive_records = [r for r in records if r.amount > 0]
        assert len(positive_records) == 3
        
        amounts = {r.address: r.amount for r in positive_records}
        assert amounts["0xrecipient1"] == 300
        assert amounts["0xrecipient2"] == 200  
        assert amounts["0xrecipient3"] == 150
        
        # Check all negative records (sender debits)
        negative_records = [r for r in records if r.amount < 0]
        assert len(negative_records) == 3
        
        for neg_record in negative_records:
            assert neg_record.address == "0xmulti_sender"
            assert neg_record.amount in [-300, -200, -150]


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
        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xzero_sender",
            whitelisted_addresses=[],
            message_hash="zero_amount_transfer",
        )
        session.commit()
        
        records = session.query(AlephCreditBalanceDb).filter_by(
            credit_ref="zero_amount_transfer"
        ).all()
        
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
        update_credit_balances_transfer(
            session=session,
            credits_list=credits_list,
            sender_address="0xself_transfer_user",  # Same as recipient
            whitelisted_addresses=[],
            message_hash="self_transfer_test",
        )
        session.commit()
        
        records = session.query(AlephCreditBalanceDb).filter_by(
            credit_ref="self_transfer_test"
        ).all()
        
        # Should create both positive and negative records
        assert len(records) == 2
        
        amounts = [r.amount for r in records]
        assert 250 in amounts   # Positive entry
        assert -250 in amounts  # Negative entry
        
        # Both records should have same address
        for record in records:
            assert record.address == "0xself_transfer_user"