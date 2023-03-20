"""
Schemas for the generic Aleph message indexer.
"""

import datetime as dt
from enum import Enum
from typing import List, Any, Tuple

from pydantic import BaseModel, Field, validator


class IndexerBlockchain(str, Enum):
    BSC = "bsc"
    ETHEREUM = "ethereum"
    SOLANA = "solana"


class EntityType(str, Enum):
    BLOCK = "block"
    TRANSACTION = "transaction"
    LOG = "log"
    STATE = "state"


class AccountEntityState(BaseModel):
    blockchain: IndexerBlockchain
    type: EntityType
    indexer: str
    account: str
    accurate: bool
    progress: float
    pending: List[Tuple[dt.datetime, dt.datetime]]
    processed: List[Tuple[dt.datetime, dt.datetime]]

    @validator("pending", "processed", pre=True, each_item=True)
    def split_datetime_ranges(cls, v):
        if isinstance(v, str):
            return v.split("/")
        return v


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
