from typing import List, Optional

from aleph_message.models import Chain, ItemHash, MessageType
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from aleph.types.message_status import MessageStatus
from aleph.types.sort_order import SortBy, SortOrder

DEFAULT_WS_HISTORY = 10
DEFAULT_MESSAGES_PER_PAGE = 20
DEFAULT_PAGE = 1
LIST_FIELD_SEPARATOR = ","


class BaseMessageQueryParams(BaseModel):
    sort_by: SortBy = Field(
        default=SortBy.TIME,
        alias="sortBy",
        description="Key to use to sort the messages. "
        "'time' uses the message time field. "
        "'tx-time' uses the first on-chain confirmation time.",
    )
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        alias="sortOrder",
        description="Order in which messages should be listed: "
        "-1 means most recent messages first, 1 means older messages first.",
    )
    message_type: Optional[MessageType] = Field(
        default=None,
        alias="msgType",
        description="Message type. Deprecated: use msgTypes instead",
    )
    message_types: Optional[List[MessageType]] = Field(
        default=None, alias="msgTypes", description="Accepted message types."
    )
    message_statuses: Optional[List[MessageStatus]] = Field(
        default=[MessageStatus.PROCESSED, MessageStatus.REMOVING],
        alias="msgStatuses",
        description="Accepted values for the 'status' field.",
    )
    addresses: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'sender' field."
    )
    refs: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.ref' field."
    )
    content_hashes: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentHashes",
        description="Accepted values for the 'content.item_hash' field.",
    )
    content_keys: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentKeys",
        description="Accepted values for the 'content.keys' field.",
    )
    content_types: Optional[List[str]] = Field(
        default=None,
        alias="contentTypes",
        description="Accepted values for the 'content.type' field.",
    )
    chains: Optional[List[Chain]] = Field(
        default=None, description="Accepted values for the 'chain' field."
    )
    channels: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'channel' field."
    )
    tags: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.content.tag' field."
    )
    hashes: Optional[List[ItemHash]] = Field(
        default=None, description="Accepted values for the 'item_hash' field."
    )

    start_date: float = Field(
        default=0,
        ge=0,
        alias="startDate",
        description="Start date timestamp. If specified, only messages with "
        "a time field greater or equal to this value will be returned.",
    )
    end_date: float = Field(
        default=0,
        ge=0,
        alias="endDate",
        description="End date timestamp. If specified, only messages with "
        "a time field lower than this value will be returned.",
    )

    start_block: int = Field(
        default=0,
        ge=0,
        alias="startBlock",
        description="Start block number. If specified, only messages with "
        "a block number greater or equal to this value will be returned.",
    )
    end_block: int = Field(
        default=0,
        ge=0,
        alias="endBlock",
        description="End block number. If specified, only messages with "
        "a block number lower than this value will be returned.",
    )

    @model_validator(mode="after")
    def validate_field_dependencies(self):
        start_date = self.start_date
        end_date = self.end_date
        if start_date and end_date and (end_date < start_date):
            raise ValueError("end date cannot be lower than start date.")
        start_block = self.start_block
        end_block = self.end_block
        if start_block and end_block and (end_block < start_block):
            raise ValueError("end block cannot be lower than start block.")

        return self

    @field_validator(
        "hashes",
        "addresses",
        "refs",
        "content_hashes",
        "content_keys",
        "content_types",
        "chains",
        "channels",
        "message_types",
        "message_statuses",
        "tags",
        mode="before",
    )
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v

    model_config = ConfigDict(populate_by_name=True)


class MessageQueryParams(BaseMessageQueryParams):
    pagination: int = Field(
        default=DEFAULT_MESSAGES_PER_PAGE,
        ge=0,
        description="Maximum number of messages to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )


class WsMessageQueryParams(BaseMessageQueryParams):
    history: Optional[int] = Field(
        DEFAULT_WS_HISTORY,
        ge=0,
        lt=200,
        description="Historical elements to send through the websocket.",
    )


class MessageHashesQueryParams(BaseModel):
    status: Optional[MessageStatus] = Field(
        default=None,
        description="Message status.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )
    pagination: int = Field(
        default=DEFAULT_MESSAGES_PER_PAGE,
        ge=0,
        description="Maximum number of messages to return. Specifying 0 removes this limit.",
    )
    start_date: float = Field(
        default=0,
        ge=0,
        alias="startDate",
        description="Start date timestamp. If specified, only messages with "
        "a time field greater or equal to this value will be returned.",
    )
    end_date: float = Field(
        default=0,
        ge=0,
        alias="endDate",
        description="End date timestamp. If specified, only messages with "
        "a time field lower than this value will be returned.",
    )
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        alias="sortOrder",
        description="Order in which messages should be listed: "
        "-1 means most recent messages first, 1 means older messages first.",
    )
    hash_only: bool = Field(
        default=True,
        description="By default, only hashes are returned. "
        "Set this to false to include metadata alongside the hashes in the response.",
    )

    @model_validator(mode="after")
    def validate_field_dependencies(self):
        start_date = self.start_date
        end_date = self.end_date
        if start_date and end_date and (end_date < start_date):
            raise ValueError("end date cannot be lower than start date.")
        return self

    model_config = ConfigDict(populate_by_name=True)
