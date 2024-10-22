import datetime as dt
from decimal import Decimal
from typing import Dict, List, Optional

from aleph_message.models import Chain
from pydantic import BaseModel, Field

from aleph.types.files import FileType
from aleph.types.sort_order import SortOrder
from aleph.web.controllers.utils import DEFAULT_PAGE


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


class GetAccountFilesResponseItem(BaseModel):
    file_hash: str
    size: int
    type: FileType
    created: dt.datetime
    item_hash: str


class GetAccountFilesResponse(BaseModel):
    class Config:
        orm_mode = True

    address: str
    total_size: int
    files: List[GetAccountFilesResponseItem]
    pagination_page: int
    pagination_total: int
    pagination_per_page: int
