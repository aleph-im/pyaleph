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


import json
from hashlib import sha256
from typing import Any, Literal, Optional, Union

from aleph_message.models import (
    AggregateContent,
    BaseContent,
    ForgetContent,
    PostContent,
    ProgramContent,
    StoreContent,
    HashType,
)
from aleph_message.models import MessageType, ItemType
from pydantic import BaseModel, root_validator, validator

from aleph.exceptions import InvalidMessageError
from aleph.utils import item_type_from_hash

MAX_INLINE_SIZE = 200000  # 200kb max inline content size.


class BasePendingMessage(BaseModel):
    """
    A raw Aleph message, as sent by users to the Aleph network.
    """

    sender: str
    chain: str
    signature: str
    type: MessageType
    item_content: Optional[str]
    item_type: ItemType
    item_hash: str
    time: float
    channel: Optional[str] = None
    content: Optional[BaseContent] = None

    @root_validator(pre=True)
    def load_content(cls, values):
        """
        Preload inline content. We let the CCN populate this field later
        on for ipfs and storage item types.
        """

        item_type = values["item_type"]
        item_content = values.get("item_content")
        if item_type == ItemType.inline:
            if item_content is None:
                raise ValueError("Item content not specified for inline item type")

            if len(item_content) > MAX_INLINE_SIZE:
                raise ValueError("Message too long")
            try:
                values["content"] = json.loads(item_content)
            except json.JSONDecodeError as e:
                raise ValueError("Could not load item content") from e
        else:
            if item_content is not None:
                raise ValueError(f"{item_type} messages cannot define item_content")

        return values

    @root_validator()
    def check_item_type(cls, values):
        """
        Checks that the item hash of the message matches the one inferred from the hash.
        Only applicable to storage/ipfs item types.
        """
        item_type = ItemType(values["item_type"])
        if item_type == ItemType.inline:
            return values

        expected_item_type = item_type_from_hash(values["item_hash"])
        if item_type != expected_item_type:
            raise ValueError(
                f"Expected {expected_item_type} based on hash but item type is {item_type}."
            )
        return values

    @validator("item_hash")
    def check_item_hash(cls, v, values):
        """
        For inline item types, check that the item hash is equal to
        the hash of the item content.
        """

        item_type = values["item_type"]
        if item_type == ItemType.inline:
            item_content: str = values["item_content"]

            computed_hash: str = sha256(item_content.encode()).hexdigest()
            if v != computed_hash:
                raise ValueError(
                    "'item_hash' do not match 'sha256(item_content)'"
                    f", expecting {computed_hash}"
                )
        elif item_type == ItemType.ipfs:
            # TODO: CHeck that the hash looks like an IPFS multihash
            pass
        else:
            assert item_type == ItemType.storage
        return v


class PendingAggregateMessage(BasePendingMessage):
    type: Literal[MessageType.aggregate]  # type: ignore
    content: Optional[AggregateContent] = None


class PendingForgetMessage(BasePendingMessage):
    type: Literal[MessageType.forget]  # type: ignore
    content: Optional[ForgetContent] = None


class PendingPostMessage(BasePendingMessage):
    type: Literal[MessageType.post]  # type: ignore
    content: Optional[PostContent] = None


class PendingProgramMessage(BasePendingMessage):
    type: Literal[MessageType.program]  # type: ignore
    content: Optional[ProgramContent] = None


class PendingStoreMessage(BasePendingMessage):
    type: Literal[MessageType.store]  # type: ignore
    content: Optional[StoreContent] = None


MESSAGE_TYPE_TO_CLASS = {
    MessageType.aggregate: PendingAggregateMessage,
    MessageType.forget: PendingForgetMessage,
    MessageType.post: PendingPostMessage,
    MessageType.program: PendingProgramMessage,
    MessageType.store: PendingStoreMessage,
}


def parse_message(message_dict: Any) -> BasePendingMessage:
    if not isinstance(message_dict, dict):
        raise InvalidMessageError("Message is not a dictionary")

    raw_message_type = message_dict.get("type")
    try:
        message_type = MessageType(raw_message_type)
    except ValueError as e:
        raise InvalidMessageError(f"Invalid message_type: '{raw_message_type}'") from e

    msg_cls = MESSAGE_TYPE_TO_CLASS[message_type]

    try:
        return msg_cls(**message_dict)
    except ValueError as e:
        raise InvalidMessageError("Could not parse message") from e
