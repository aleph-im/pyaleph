import pytest
from pydantic import ValidationError

from aleph.schemas.credit_transfer import CreditTransferContent, CreditTransferEntry


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
