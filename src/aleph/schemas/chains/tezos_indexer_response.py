import datetime as dt
from enum import Enum
from typing import Generic, List, TypeVar

from pydantic.v1 import BaseModel, Field
from pydantic.v1.generics import GenericModel

PayloadType = TypeVar("PayloadType")


class SyncStatus(str, Enum):
    SYNCED = "synced"
    IN_PROGRESS = "in_progress"
    DOWN = "down"


class IndexerStatus(BaseModel):
    oldest_block: str = Field(alias="oldestBlock")
    recent_block: str = Field(alias="recentBlock")
    status: SyncStatus


class IndexerStats(BaseModel):
    total_events: int = Field(alias="totalEvents")


class IndexerEvent(GenericModel, Generic[PayloadType]):
    source: str
    timestamp: dt.datetime
    block_level: int = Field(alias="blockLevel")
    operation_hash: str = Field(alias="operationHash")
    type: str
    payload: PayloadType


class MessageEventPayload(BaseModel):
    class Config:
        allow_population_by_field_name = True

    timestamp: float
    addr: str
    message_type: str = Field(alias="msgtype")
    message_content: str = Field(alias="msgcontent")

    # The following properties are defined for interoperability with the generic
    # MessageEvent class.
    @property
    def address(self) -> str:
        return self.addr

    @property
    def type(self) -> str:
        return self.message_type

    @property
    def content(self) -> str:
        return self.message_content

    @property
    def timestamp_seconds(self) -> float:
        return self.timestamp


IndexerMessageEvent = IndexerEvent[MessageEventPayload]


IndexerEventType = TypeVar("IndexerEventType", bound=IndexerEvent)


class IndexerResponseData(GenericModel, Generic[IndexerEventType]):
    index_status: IndexerStatus = Field(alias="indexStatus")
    stats: IndexerStats
    events: List[IndexerEventType]


class IndexerResponse(GenericModel, Generic[IndexerEventType]):
    data: IndexerResponseData[IndexerEventType]
