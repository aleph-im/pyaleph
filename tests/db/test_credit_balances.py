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