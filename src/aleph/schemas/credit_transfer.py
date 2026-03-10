from decimal import Decimal, InvalidOperation
from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


def _validate_address(v: str) -> str:
    if not v.strip():
        raise ValueError("address must not be empty")
    return v


def _validate_positive_int_amount(v: object) -> int:
    if not isinstance(v, int) or isinstance(v, bool):
        raise ValueError(f"amount must be an integer, got {type(v).__name__}")
    return v


# ---------------------------------------------------------------------------
# Transfer
# ---------------------------------------------------------------------------


class CreditTransferEntry(BaseModel):
    address: str
    amount: int = Field(gt=0, description="Amount must be a strictly positive integer")
    expiration: Optional[int] = None

    @field_validator("address")
    @classmethod
    def address_not_empty(cls, v: str) -> str:
        return _validate_address(v)

    @field_validator("amount", mode="before")
    @classmethod
    def amount_must_be_int(cls, v: object) -> int:
        return _validate_positive_int_amount(v)


class CreditTransferList(BaseModel):
    credits: List[CreditTransferEntry] = Field(min_length=1)


class CreditTransferContent(BaseModel):
    transfer: CreditTransferList

    @model_validator(mode="after")
    def no_duplicate_recipients(self) -> "CreditTransferContent":
        addresses = [e.address for e in self.transfer.credits]
        if len(addresses) != len(set(addresses)):
            raise ValueError(
                "Duplicate recipient addresses are not allowed in a single transfer"
            )
        return self


# ---------------------------------------------------------------------------
# Distribution
# ---------------------------------------------------------------------------


class CreditDistributionEntry(BaseModel):
    address: str
    amount: int = Field(gt=0)
    price: str
    tx_hash: str
    provider: str
    expiration: Optional[int] = None
    origin: Optional[str] = None
    ref: Optional[str] = None
    payment_method: Optional[str] = None
    bonus_amount: Optional[Any] = None

    @field_validator("address")
    @classmethod
    def address_not_empty(cls, v: str) -> str:
        return _validate_address(v)

    @field_validator("amount", mode="before")
    @classmethod
    def amount_must_be_int(cls, v: object) -> int:
        return _validate_positive_int_amount(v)

    @field_validator("price", mode="before")
    @classmethod
    def coerce_price_to_str(cls, v: object) -> object:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return str(v)
        return v

    @field_validator("price")
    @classmethod
    def price_must_be_decimal(cls, v: str) -> str:
        try:
            Decimal(v)
        except InvalidOperation:
            raise ValueError(f"price must be a valid decimal string, got {v!r}")
        return v


class CreditDistributionList(BaseModel):
    credits: List[CreditDistributionEntry] = Field(min_length=1)
    token: str
    chain: str


class CreditDistributionContent(BaseModel):
    distribution: CreditDistributionList


# ---------------------------------------------------------------------------
# Expense
# ---------------------------------------------------------------------------


class CreditExpenseEntry(BaseModel):
    address: str
    amount: int = Field(gt=0)
    ref: Optional[str] = None
    execution_id: Optional[str] = None
    node_id: Optional[str] = None
    price: Optional[str] = None
    time: Optional[int] = None  # accepted but ignored

    @field_validator("address")
    @classmethod
    def address_not_empty(cls, v: str) -> str:
        return _validate_address(v)

    @field_validator("amount", mode="before")
    @classmethod
    def amount_must_be_int(cls, v: object) -> int:
        return _validate_positive_int_amount(v)

    @field_validator("price", mode="before")
    @classmethod
    def coerce_price_to_str(cls, v: object) -> object:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return str(v)
        return v

    @field_validator("price")
    @classmethod
    def price_must_be_decimal_or_none(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                Decimal(v)
            except InvalidOperation:
                raise ValueError(f"price must be a valid decimal string, got {v!r}")
        return v


class CreditExpenseList(BaseModel):
    credits: List[CreditExpenseEntry] = Field(min_length=1)


class CreditExpenseContent(BaseModel):
    expense: CreditExpenseList
