import datetime as dt
from decimal import Decimal
from typing import Annotated, Dict, List, Optional

from aleph_message.models import Chain
from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, field_validator

from aleph.schemas.messages_query_params import DEFAULT_PAGE, LIST_FIELD_SEPARATOR
from aleph.types.files import FileType
from aleph.types.sort_order import SortOrder


class GetAccountQueryParams(BaseModel):
    chain: Optional[Chain] = Field(
        default=None, description="Get Balance on a specific EVM Chain"
    )


FloatDecimal = Annotated[
    Decimal, PlainSerializer(lambda x: float(x), return_type=float, when_used="always")
]


class GetAccountBalanceResponse(BaseModel):
    address: str
    balance: FloatDecimal
    details: Optional[Dict[str, FloatDecimal]] = None
    locked_amount: FloatDecimal
    credit_balance: int = 0


class GetAccountFilesQueryParams(BaseModel):
    pagination: int = Field(
        default=100,
        ge=0,
        description="Maximum number of files to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        description="Order in which files should be listed: "
        "-1 means most recent messages first, 1 means older messages first.",
    )


class GetBalancesChainsQueryParams(BaseModel):
    chains: Optional[List[Chain]] = Field(
        default=None, description="Accepted values for the 'chain' field."
    )
    pagination: int = Field(
        default=100,
        ge=0,
        description="Maximum number of files to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )
    min_balance: int = Field(default=0, ge=1, description="Minimum Balance needed")

    @field_validator("chains", mode="before")
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v


class AddressBalanceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    address: str
    balance: FloatDecimal
    chain: Chain


class GetCreditBalancesQueryParams(BaseModel):
    pagination: int = Field(
        default=100,
        ge=0,
        description="Maximum number of credit balances to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )
    min_balance: int = Field(
        default=0, ge=1, description="Minimum Credit Balance needed"
    )


class AddressCreditBalanceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    address: str
    credits: int


class GetAccountFilesResponseItem(BaseModel):
    file_hash: str
    size: int
    type: FileType
    created: dt.datetime
    item_hash: str


class GetAccountFilesResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    address: str
    total_size: int
    files: List[GetAccountFilesResponseItem]
    pagination_page: int
    pagination_total: int
    pagination_per_page: int


class GetAccountCreditHistoryQueryParams(BaseModel):
    pagination: int = Field(
        default=0,
        ge=0,
        description="Maximum number of credit history entries to return. Specifying 0 returns all entries.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )
    tx_hash: Optional[str] = Field(
        default=None, description="Filter by transaction hash"
    )
    token: Optional[str] = Field(default=None, description="Filter by token")
    chain: Optional[str] = Field(default=None, description="Filter by chain")
    provider: Optional[str] = Field(default=None, description="Filter by provider")
    origin: Optional[str] = Field(default=None, description="Filter by origin")
    origin_ref: Optional[str] = Field(
        default=None, description="Filter by origin reference"
    )
    payment_method: Optional[str] = Field(
        default=None, description="Filter by payment method"
    )


class CreditHistoryResponseItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    amount: int
    price: Optional[Decimal] = None
    tx_hash: Optional[str] = None
    token: Optional[str] = None
    chain: Optional[str] = None
    provider: Optional[str] = None
    origin: Optional[str] = None
    origin_ref: Optional[str] = None
    payment_method: Optional[str] = None
    credit_ref: str
    credit_index: int
    expiration_date: Optional[dt.datetime] = None
    message_timestamp: dt.datetime


class GetAccountCreditHistoryResponse(BaseModel):
    address: str
    credit_history: List[CreditHistoryResponseItem]
    pagination_page: int
    pagination_total: int
    pagination_per_page: int


class GetResourceConsumedCreditsResponse(BaseModel):
    item_hash: str
    consumed_credits: int


class GetAccountPostTypesResponse(BaseModel):
    address: str
    post_types: List[str]


class GetAccountChannelsResponse(BaseModel):
    address: str
    channels: List[str]
