from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class CreditTransferEntry(BaseModel):
    address: str
    amount: int = Field(gt=0, description="Amount must be a strictly positive integer")
    expiration: Optional[int] = None

    @field_validator("address")
    @classmethod
    def address_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Recipient address must not be empty")
        return v

    @field_validator("amount", mode="before")
    @classmethod
    def amount_must_be_int(cls, v: object) -> int:
        if not isinstance(v, int) or isinstance(v, bool):
            raise ValueError(f"amount must be an integer, got {type(v).__name__}")
        return v


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
