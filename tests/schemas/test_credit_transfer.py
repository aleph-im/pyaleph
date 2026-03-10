import pytest
from pydantic import ValidationError

from aleph.schemas.credit_transfer import (
    CreditDistributionContent,
    CreditExpenseContent,
    CreditTransferContent,
    CreditTransferEntry,
)


class TestCreditTransferEntry:
    def test_valid_entry(self):
        e = CreditTransferEntry(address="0xabc", amount=100)
        assert e.address == "0xabc"
        assert e.amount == 100
        assert e.expiration is None

    def test_valid_entry_with_expiration(self):
        e = CreditTransferEntry(address="0xabc", amount=1, expiration=1700000000000)
        assert e.expiration == 1700000000000

    def test_zero_amount_rejected(self):
        with pytest.raises(ValidationError, match="greater than 0"):
            CreditTransferEntry(address="0xabc", amount=0)

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError, match="greater than 0"):
            CreditTransferEntry(address="0xabc", amount=-1)

    def test_negative_large_amount_rejected(self):
        with pytest.raises(ValidationError, match="greater than 0"):
            CreditTransferEntry(address="0xabc", amount=-9999999)

    def test_float_amount_rejected(self):
        with pytest.raises(ValidationError, match="integer"):
            CreditTransferEntry(address="0xabc", amount=1.5)

    def test_string_amount_rejected(self):
        with pytest.raises(ValidationError, match="integer"):
            CreditTransferEntry(address="0xabc", amount="100")

    def test_boolean_amount_rejected(self):
        # bool is a subclass of int in Python — must be explicitly rejected
        with pytest.raises(ValidationError, match="integer"):
            CreditTransferEntry(address="0xabc", amount=True)

    def test_none_amount_rejected(self):
        with pytest.raises(ValidationError):
            CreditTransferEntry(address="0xabc", amount=None)

    def test_empty_address_rejected(self):
        with pytest.raises(ValidationError, match="not be empty"):
            CreditTransferEntry(address="   ", amount=100)

    def test_missing_address_rejected(self):
        with pytest.raises(ValidationError):
            CreditTransferEntry(amount=100)

    def test_missing_amount_rejected(self):
        with pytest.raises(ValidationError):
            CreditTransferEntry(address="0xabc")


class TestCreditTransferContent:
    def test_valid_content(self):
        content = CreditTransferContent.model_validate(
            {"transfer": {"credits": [{"address": "0xabc", "amount": 100}]}}
        )
        assert len(content.transfer.credits) == 1

    def test_multiple_valid_recipients(self):
        content = CreditTransferContent.model_validate(
            {
                "transfer": {
                    "credits": [
                        {"address": "0xaaa", "amount": 50},
                        {"address": "0xbbb", "amount": 75},
                    ]
                }
            }
        )
        assert len(content.transfer.credits) == 2

    def test_empty_credits_list_rejected(self):
        with pytest.raises(ValidationError, match="at least 1"):
            CreditTransferContent.model_validate({"transfer": {"credits": []}})

    def test_missing_transfer_key_rejected(self):
        with pytest.raises(ValidationError):
            CreditTransferContent.model_validate({"foo": "bar"})

    def test_missing_credits_key_rejected(self):
        with pytest.raises(ValidationError):
            CreditTransferContent.model_validate({"transfer": {}})

    def test_duplicate_recipients_rejected(self):
        with pytest.raises(ValidationError, match="Duplicate"):
            CreditTransferContent.model_validate(
                {
                    "transfer": {
                        "credits": [
                            {"address": "0xabc", "amount": 10},
                            {"address": "0xabc", "amount": 20},
                        ]
                    }
                }
            )

    def test_negative_amount_in_list_rejected(self):
        with pytest.raises(ValidationError):
            CreditTransferContent.model_validate(
                {"transfer": {"credits": [{"address": "0xabc", "amount": -50}]}}
            )

    def test_non_numeric_amount_in_list_rejected(self):
        with pytest.raises(ValidationError):
            CreditTransferContent.model_validate(
                {"transfer": {"credits": [{"address": "0xabc", "amount": "lots"}]}}
            )


_VALID_DIST_ENTRY = {
    "address": "0xabc",
    "amount": 100,
    "price": "0.5",
    "tx_hash": "0xtx",
    "provider": "test",
}


class TestCreditDistributionContent:
    def test_valid_content(self):
        content = CreditDistributionContent.model_validate(
            {
                "distribution": {
                    "credits": [_VALID_DIST_ENTRY],
                    "token": "ALEPH",
                    "chain": "ETH",
                }
            }
        )
        assert len(content.distribution.credits) == 1
        assert content.distribution.token == "ALEPH"

    def test_optional_fields_default_to_none(self):
        entry = CreditDistributionContent.model_validate(
            {
                "distribution": {
                    "credits": [_VALID_DIST_ENTRY],
                    "token": "ALEPH",
                    "chain": "ETH",
                }
            }
        ).distribution.credits[0]
        assert entry.expiration is None
        assert entry.origin is None
        assert entry.ref is None
        assert entry.payment_method is None

    def test_zero_amount_rejected(self):
        with pytest.raises(ValidationError, match="greater than 0"):
            CreditDistributionContent.model_validate(
                {
                    "distribution": {
                        "credits": [{**_VALID_DIST_ENTRY, "amount": 0}],
                        "token": "ALEPH",
                        "chain": "ETH",
                    }
                }
            )

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError):
            CreditDistributionContent.model_validate(
                {
                    "distribution": {
                        "credits": [{**_VALID_DIST_ENTRY, "amount": -1}],
                        "token": "ALEPH",
                        "chain": "ETH",
                    }
                }
            )

    def test_string_amount_rejected(self):
        with pytest.raises(ValidationError, match="integer"):
            CreditDistributionContent.model_validate(
                {
                    "distribution": {
                        "credits": [{**_VALID_DIST_ENTRY, "amount": "100"}],
                        "token": "ALEPH",
                        "chain": "ETH",
                    }
                }
            )

    def test_numeric_price_accepted(self):
        entry = CreditDistributionContent.model_validate(
            {
                "distribution": {
                    "credits": [{**_VALID_DIST_ENTRY, "price": 0.000001}],
                    "token": "ALEPH",
                    "chain": "ETH",
                }
            }
        ).distribution.credits[0]
        assert entry.price == "1e-06"

    def test_integer_price_accepted(self):
        entry = CreditDistributionContent.model_validate(
            {
                "distribution": {
                    "credits": [{**_VALID_DIST_ENTRY, "price": 1}],
                    "token": "ALEPH",
                    "chain": "ETH",
                }
            }
        ).distribution.credits[0]
        assert entry.price == "1"

    def test_invalid_price_rejected(self):
        with pytest.raises(ValidationError, match="decimal"):
            CreditDistributionContent.model_validate(
                {
                    "distribution": {
                        "credits": [{**_VALID_DIST_ENTRY, "price": "not-a-number"}],
                        "token": "ALEPH",
                        "chain": "ETH",
                    }
                }
            )

    def test_empty_credits_rejected(self):
        with pytest.raises(ValidationError, match="at least 1"):
            CreditDistributionContent.model_validate(
                {"distribution": {"credits": [], "token": "ALEPH", "chain": "ETH"}}
            )

    def test_missing_token_rejected(self):
        with pytest.raises(ValidationError):
            CreditDistributionContent.model_validate(
                {"distribution": {"credits": [_VALID_DIST_ENTRY], "chain": "ETH"}}
            )

    def test_missing_chain_rejected(self):
        with pytest.raises(ValidationError):
            CreditDistributionContent.model_validate(
                {"distribution": {"credits": [_VALID_DIST_ENTRY], "token": "ALEPH"}}
            )

    def test_empty_address_rejected(self):
        with pytest.raises(ValidationError, match="not be empty"):
            CreditDistributionContent.model_validate(
                {
                    "distribution": {
                        "credits": [{**_VALID_DIST_ENTRY, "address": "  "}],
                        "token": "ALEPH",
                        "chain": "ETH",
                    }
                }
            )


class TestCreditExpenseContent:
    def test_valid_minimal(self):
        content = CreditExpenseContent.model_validate(
            {"expense": {"credits": [{"address": "0xabc", "amount": 500}]}}
        )
        assert len(content.expense.credits) == 1

    def test_valid_with_all_fields(self):
        entry = CreditExpenseContent.model_validate(
            {
                "expense": {
                    "credits": [
                        {
                            "address": "0xabc",
                            "amount": 500,
                            "ref": "ref123",
                            "execution_id": "exec1",
                            "node_id": "node1",
                            "price": "0.001",
                            "time": 1640995200000,
                        }
                    ]
                }
            }
        ).expense.credits[0]
        assert entry.ref == "ref123"
        assert entry.execution_id == "exec1"
        assert entry.node_id == "node1"
        assert entry.price == "0.001"
        assert entry.time == 1640995200000

    def test_zero_amount_rejected(self):
        with pytest.raises(ValidationError, match="greater than 0"):
            CreditExpenseContent.model_validate(
                {"expense": {"credits": [{"address": "0xabc", "amount": 0}]}}
            )

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError):
            CreditExpenseContent.model_validate(
                {"expense": {"credits": [{"address": "0xabc", "amount": -10}]}}
            )

    def test_string_amount_rejected(self):
        with pytest.raises(ValidationError, match="integer"):
            CreditExpenseContent.model_validate(
                {"expense": {"credits": [{"address": "0xabc", "amount": "big"}]}}
            )

    def test_numeric_price_accepted(self):
        entry = CreditExpenseContent.model_validate(
            {
                "expense": {
                    "credits": [{"address": "0xabc", "amount": 10, "price": 0.001}]
                }
            }
        ).expense.credits[0]
        assert entry.price == "0.001"

    def test_invalid_price_rejected(self):
        with pytest.raises(ValidationError, match="decimal"):
            CreditExpenseContent.model_validate(
                {
                    "expense": {
                        "credits": [
                            {"address": "0xabc", "amount": 10, "price": "not-a-number"}
                        ]
                    }
                }
            )

    def test_empty_credits_rejected(self):
        with pytest.raises(ValidationError, match="at least 1"):
            CreditExpenseContent.model_validate({"expense": {"credits": []}})

    def test_empty_address_rejected(self):
        with pytest.raises(ValidationError, match="not be empty"):
            CreditExpenseContent.model_validate(
                {"expense": {"credits": [{"address": "", "amount": 10}]}}
            )
