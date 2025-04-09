"""
Schemas for the generic Aleph message indexer.
"""

import datetime as dt
from enum import Enum
from typing import Annotated, List, Protocol, Tuple

from pydantic import BaseModel, BeforeValidator, Field


class GenericMessageEvent(Protocol):
    @property
    def address(self) -> str: ...
    @property
    def type(self) -> str: ...
    @property
    def content(self) -> str: ...
    @property
    def timestamp_seconds(self) -> float: ...


class IndexerBlockchain(str, Enum):
    BSC = "bsc"
    ETHEREUM = "ethereum"
    SOLANA = "solana"


class EntityType(str, Enum):
    BLOCK = "block"
    TRANSACTION = "transaction"
    LOG = "log"
    STATE = "state"


def split_datetime_ranges(v):
    if isinstance(v, str):
        return v.split("/")
    return v


DateTimeRange = Annotated[
    Tuple[dt.datetime, dt.datetime], BeforeValidator(split_datetime_ranges)
]


class AccountEntityState(BaseModel):
    blockchain: IndexerBlockchain
    type: EntityType
    indexer: str
    account: str
    completeHistory: bool
    progress: float
    pending: List[DateTimeRange]
    processed: List[DateTimeRange]


class IndexerAccountStateResponseData(BaseModel):
    state: List[AccountEntityState]


class IndexerAccountStateResponse(BaseModel):
    data: IndexerAccountStateResponseData


class IndexerEvent(BaseModel):
    id: str
    timestamp: float
    address: str
    height: int
    transaction: str

    @property
    def timestamp_seconds(self) -> float:
        return self.timestamp / 1000


class MessageEvent(IndexerEvent):
    type: str
    content: str


class SyncEvent(IndexerEvent):
    message: str


class IndexerEventResponseData(BaseModel):
    message_events: List[MessageEvent] = Field(
        alias="messageEvents", default_factory=list
    )
    sync_events: List[SyncEvent] = Field(alias="syncEvents", default_factory=list)


class IndexerEventResponse(BaseModel):
    data: IndexerEventResponseData
