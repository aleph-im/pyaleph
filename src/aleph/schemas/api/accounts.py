import datetime as dt
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from aleph_message.models import Chain

from aleph.types.files import FileType
from aleph.types.sort_order import SortOrder
from aleph.web.controllers.utils import DEFAULT_PAGE, LIST_FIELD_SEPARATOR


class GetAccountQueryParams(BaseModel):
    chain: Optional[Chain] = Field(
        default=None, description="Get Balance on a specific EVM Chain"
    )


class GetAccountBalanceResponse(BaseModel):
    address: str
    balance: Decimal
    details: Optional[Dict[str, Decimal]]
    locked_amount: Decimal


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
    balance: str
    chain: Chain


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
