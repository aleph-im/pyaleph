"""
Base (abstract) class for messages.
"""

import datetime as dt
from hashlib import sha256
from typing import Any, Generic, Mapping, Optional, TypeVar, cast

from aleph_message.models import BaseContent, Chain, ItemType, MessageType
from pydantic import BaseModel, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo

from aleph.utils import item_type_from_hash

MType = TypeVar("MType", bound=MessageType)
ContentType = TypeVar("ContentType", bound=BaseContent)


def base_message_validator_check_item_type(values):
    """
    Checks that the item hash of the message matches the one inferred from the hash.
    Only applicable to storage/ipfs item types.
    """
    item_type_value = values.get("item_type")
    if item_type_value is None:
        raise ValueError("Could not determine item type")

    item_type = ItemType(item_type_value)
    if item_type == ItemType.inline:
        return values

    item_hash = values.get("item_hash")
    if item_hash is None:
        raise ValueError("Could not determine item hash")

    expected_item_type = item_type_from_hash(item_hash)
    if item_type != expected_item_type:
        raise ValueError(
            f"Expected {expected_item_type} based on hash but item type is {item_type}."
        )
    return values


def base_message_validator_check_item_hash(v: Any, values: Mapping[str, Any]):
    """
    For inline item types, check that the item hash is equal to
    the hash of the item content.
    """

    item_type = values.get("item_type")
    if item_type is None:
        raise ValueError("Could not determine item type")

    if item_type == ItemType.inline:
        item_content = cast(Optional[str], values.get("item_content"))
        if item_content is None:
            raise ValueError("Could not find inline item content")

        computed_hash: str = sha256(item_content.encode()).hexdigest()
        if v != computed_hash:
            raise ValueError(
                "'item_hash' does not match 'sha256(item_content)'"
                f", expecting {computed_hash}"
            )
    elif item_type == ItemType.ipfs:
        # TODO: CHeck that the hash looks like an IPFS multihash
        pass
    else:
        if item_type != ItemType.storage:
            raise ValueError(f"Unknown item type: '{item_type}'")
    return v


class AlephBaseMessage(BaseModel, Generic[MType, ContentType]):
    """
    The base structure of an Aleph message.
    All the fields of this class appear in all the representations
    of messages on the Aleph network.
    """

    sender: Optional[str] = None
    chain: Optional[Chain] = None
    signature: Optional[str] = None
    type: Optional[MType] = None
    item_content: Optional[str] = None
    item_type: ItemType
    item_hash: str
    time: Optional[dt.datetime] = None
    channel: Optional[str] = None
    content: Optional[ContentType] = None

    @model_validator(mode="after")
    def check_item_type(self) -> "AlephBaseMessage":
        values = self.model_dump()
        base_message_validator_check_item_type(values)
        return self

    @field_validator("item_hash")
    def check_item_hash(cls, v: Any, info: ValidationInfo):
        return base_message_validator_check_item_hash(v, info.data)
