from pydantic import BaseModel
from decimal import Decimal


class GetBalanceResponse(BaseModel):
    address: str
    balance: Decimal
