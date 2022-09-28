"""
Schemas for validated messages, as stored in the messages collection.
Validated messages are fully loaded, i.e. their content field is
always present, unlike pending messages.
"""

from typing import List, Literal, Optional, Generic, Dict, Type, Any

from aleph_message.models import (
    AggregateContent,
    ForgetContent,
    MessageType,
    PostContent,
    ProgramContent,
    StoreContent, )
from pydantic import BaseModel, Field

from aleph.schemas.base_messages import AlephBaseMessage, ContentType, MType
from aleph.schemas.pending_messages import (
    BasePendingMessage,
    PendingAggregateMessage,
    PendingForgetMessage,
    PendingPostMessage,
    PendingProgramMessage,
    PendingStoreMessage,
)
from .message_confirmation import MessageConfirmation
from .message_content import MessageContent


class EngineInfo(BaseModel):
    hash: str = Field(alias="Hash")
    size: int = Field(alias="Size")
    cumulative_size: int = Field(alias="CumulativeSize")
    blocks: int = Field(alias="Blocks")
    type: str = Field(alias="Type")


class StoreContentWithMetadata(StoreContent):
    content_type: Literal["directory", "file"]
    size: int
    engine_info: Optional[EngineInfo] = None

    @classmethod
    def from_content(cls, store_content: StoreContent):
        return cls(
            address=store_content.address,
            time=store_content.time,
            item_type=store_content.item_type,
            item_hash=store_content.item_hash,
            content_type="file",
            size=0,
            engine_info=None,
        )


class BaseValidatedMessage(AlephBaseMessage, Generic[MType, ContentType]):
    confirmed: bool
    size: int
    content: ContentType
    confirmations: List[MessageConfirmation] = Field(default_factory=list)
    forgotten_by: List[str] = Field(default_factory=list)


class ValidatedAggregateMessage(
    BaseValidatedMessage[Literal[MessageType.aggregate], AggregateContent]  # type: ignore
):
    pass


class ValidatedForgetMessage(
    BaseValidatedMessage[Literal[MessageType.forget], ForgetContent]  # type: ignore
):
    pass


class ValidatedPostMessage(
    BaseValidatedMessage[Literal[MessageType.post], PostContent]  # type: ignore
):
    pass


class ValidatedProgramMessage(
    BaseValidatedMessage[Literal[MessageType.program], ProgramContent]  # type: ignore
):
    pass


class ValidatedStoreMessage(
    BaseValidatedMessage[Literal[MessageType.store], StoreContent]  # type: ignore
):
    pass


def validate_pending_message(
    pending_message: BasePendingMessage[MType, ContentType],
    content: MessageContent,
    confirmations: List[MessageConfirmation],
) -> BaseValidatedMessage[MType, ContentType]:

    type_map: Dict[Type[BasePendingMessage], Type[BaseValidatedMessage]] = {
        PendingAggregateMessage: ValidatedAggregateMessage,
        PendingForgetMessage: ValidatedForgetMessage,
        PendingPostMessage: ValidatedPostMessage,
        PendingProgramMessage: ValidatedProgramMessage,
        PendingStoreMessage: ValidatedStoreMessage,
    }

    # Some values may be missing in the content, adjust them
    json_content = content.value
    if json_content.get("address", None) is None:
        json_content["address"] = pending_message.sender

    if json_content.get("time", None) is None:
        json_content["time"] = pending_message.time

    # Note: we could use the construct method of Pydantic to bypass validation
    # and speed up the conversion process. However, this means giving up on validation.
    # At the time of writing, correctness seems more important than performance.
    return type_map[type(pending_message)](
        **pending_message.dict(exclude={"content"}),
        content=content.value,
        confirmed=bool(confirmations),
        confirmations=confirmations,
        size=len(content.raw_value),
    )


def make_confirmation_update_query(confirmations: List[MessageConfirmation]) -> Dict:
    """
    Creates a MongoDB update query that confirms an existing message.
    """

    # We use addToSet as multiple confirmations may be treated in //
    if not confirmations:
        return {"$max": {"confirmed": False}}

    return {
        "$max": {"confirmed": True},
        "$addToSet": {
            "confirmations": {
                "$each": [confirmation.dict() for confirmation in confirmations]
            }
        },
    }


def make_message_upsert_query(message: BaseValidatedMessage[Any, Any]) -> Dict:
    """
    Creates a MongoDB upsert query to insert the message in the DB.
    """

    updates = {
        "$set": {
            "content": message.content.dict(exclude_none=True),
            "size": message.size,
            "item_content": message.item_content,
            "item_type": message.item_type.value,
            "channel": message.channel,
            "signature": message.signature,
        },
        "$min": {"time": message.time},
    }

    # Add fields related to confirmations
    updates.update(make_confirmation_update_query(message.confirmations))

    return updates
