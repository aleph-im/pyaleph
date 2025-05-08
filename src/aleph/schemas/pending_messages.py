"""
Schemas to process raw messages coming from users to the Aleph network.
These schemas are used to parse messages coming from the network into
more practical Python objects.

While extremely similar to the functionalities of the aleph message module
(of which we reuse some classes), this implementation differs in several
ways:
1. We do not expect the `content` key to be provided. At best, we get
   an `item_content` field for inline type messages. Otherwise,
   the content has to be fetched (and validated) later from the network.
2. We do not care for confirmations, as the message we are checking is
   not even integrated yet.

TODO: this module should reasonably be part of aleph message, if only
      to make the schemas available for the validation of client data
      in aleph-client.
"""

import datetime as dt
from typing import Any, Dict, Generic, Literal, Type

import pydantic_core
from aleph_message.models import (
    AggregateContent,
    Chain,
    ForgetContent,
    InstanceContent,
    ItemType,
    MessageType,
    PostContent,
    ProgramContent,
    StoreContent,
)
from pydantic import ValidationError, field_validator, model_validator

import aleph.toolkit.json as aleph_json
from aleph.exceptions import UnknownHashError
from aleph.schemas.base_messages import AlephBaseMessage, ContentType, MType
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.message_status import InvalidMessageFormat
from aleph.utils import item_type_from_hash

MAX_INLINE_SIZE = 200000  # 200kb max inline content size.


def base_pending_message_load_content(values):
    """
    Preload inline content. We let the CCN populate this field later
    on for ipfs and storage item types.

    Sets the default value for item_type, if required.
    """

    item_hash = values.get("item_hash")
    if item_hash is None:
        raise ValueError("Could not determine item hash")
    item_content = values.get("item_content")

    try:
        default_item_type = (
            item_type_from_hash(item_hash) if item_content is None else ItemType.inline
        )
    except UnknownHashError:
        raise ValueError(f"Unexpected hash type: '{item_hash}'")

    input_item_type = values.get("item_type")
    item_type = input_item_type or default_item_type

    if item_type == ItemType.inline:
        if item_content is None:
            raise ValueError("Item content not specified for inline item type")

        if len(item_content) > MAX_INLINE_SIZE:
            raise ValueError("Message too long")
        try:
            values["content"] = aleph_json.loads(item_content)
        except aleph_json.DecodeError as e:
            raise ValueError("Message content is not valid JSON data") from e
    else:
        if item_content is not None:
            raise ValueError(f"{item_type} messages cannot define item_content")

    # Store back the default item_type if not specified
    if input_item_type is None:
        values["item_type"] = default_item_type.value

    return values


def base_pending_message_validator_check_time(v, values):
    """
    Parses the time field as a UTC datetime. Contrary to the default datetime
    validator, this implementation raises an exception if the time field is
    too far in the future.
    """

    if isinstance(v, dt.datetime):
        return v

    return timestamp_to_datetime(v)


class BasePendingMessage(AlephBaseMessage, Generic[MType, ContentType]):
    """
    A raw Aleph message, as sent by users to the Aleph network.
    """

    sender: str
    chain: Chain
    type: MType
    time: dt.datetime

    @model_validator(mode="before")
    def load_content(cls, values):
        return base_pending_message_load_content(values)

    @field_validator("time", mode="before")
    def check_time(cls, v, info):
        return base_pending_message_validator_check_time(v, info.data)


class PendingAggregateMessage(
    BasePendingMessage[Literal[MessageType.aggregate], AggregateContent]  # type: ignore
):
    pass


class PendingForgetMessage(
    BasePendingMessage[Literal[MessageType.forget], ForgetContent]  # type: ignore
):
    pass


class PendingInstanceMessage(
    BasePendingMessage[Literal[MessageType.instance], InstanceContent]  # type: ignore
):
    pass


class PendingPostMessage(BasePendingMessage[Literal[MessageType.post], PostContent]):  # type: ignore
    pass


class PendingProgramMessage(
    BasePendingMessage[Literal[MessageType.program], ProgramContent]  # type: ignore
):
    pass


class PendingStoreMessage(BasePendingMessage[Literal[MessageType.store], StoreContent]):  # type: ignore
    pass


class PendingInlineStoreMessage(PendingStoreMessage):
    item_content: str
    item_type: Literal[ItemType.inline]  # type: ignore


MESSAGE_TYPE_TO_CLASS: Dict[
    Any,
    Type[
        PendingAggregateMessage
        | PendingForgetMessage
        | PendingInstanceMessage
        | PendingPostMessage
        | PendingProgramMessage
        | PendingStoreMessage
    ],
] = {
    MessageType.aggregate: PendingAggregateMessage,
    MessageType.forget: PendingForgetMessage,
    MessageType.instance: PendingInstanceMessage,
    MessageType.post: PendingPostMessage,
    MessageType.program: PendingProgramMessage,
    MessageType.store: PendingStoreMessage,
}


def parse_message(message_dict: Any) -> BasePendingMessage:
    if not isinstance(message_dict, dict):
        raise InvalidMessageFormat("Message is not a dictionary")

    raw_message_type = message_dict.get("type")
    try:
        message_type = MessageType(raw_message_type)
    except ValueError as e:
        raise InvalidMessageFormat(f"Invalid message_type: '{raw_message_type}'") from e

    msg_cls = MESSAGE_TYPE_TO_CLASS[message_type]

    try:
        return msg_cls(**message_dict)
    except (ValidationError, pydantic_core.ValidationError) as e:
        raise InvalidMessageFormat(e.errors()) from e
