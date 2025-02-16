from typing import Any, Dict, Generic, Literal, Type, TypeAlias
from aiohttp import web
from pydantic import ValidationError, root_validator

from aleph_message.models import (
    InstanceContent,
    MessageType,
    ProgramContent,
    StoreContent,
)
from aleph.schemas.base_messages import AlephBaseMessage, ContentType, MType
from aleph.schemas.pending_messages import base_pending_message_load_content
from aleph.storage import StorageService
from aleph.types.message_status import InvalidMessageException, InvalidMessageFormat


class CostEstimationMessage(AlephBaseMessage, Generic[MType, ContentType]):
    """
    A raw Aleph message, sent by users to estimate costs before reaching the network
    """

    type: MType
    item_hash: str

    @root_validator(pre=True)
    def load_content(cls, values):
        return base_pending_message_load_content(values)


class CostEstimationInstanceMessage(
    CostEstimationMessage[Literal[MessageType.instance], InstanceContent]  # type: ignore
):
    pass


class CostEstimationProgramMessage(
    CostEstimationMessage[Literal[MessageType.program], ProgramContent]  # type: ignore
):
    pass


class CostEstimationStoreMessage(CostEstimationMessage[Literal[MessageType.store], StoreContent]):  # type: ignore
    pass


CostEstimationMessage: TypeAlias = (
    CostEstimationInstanceMessage
    | CostEstimationProgramMessage
    | CostEstimationStoreMessage
)

CostEstimationMessageContent: TypeAlias = (
    InstanceContent | ProgramContent | StoreContent
)


COST_MESSAGE_TYPE_TO_CLASS: Dict[
    MessageType,
    Type[CostEstimationMessage],
] = {
    MessageType.instance: CostEstimationInstanceMessage,
    MessageType.program: CostEstimationProgramMessage,
    MessageType.store: CostEstimationStoreMessage,
}


COST_MESSAGE_TYPE_TO_CONTENT: Dict[MessageType, Type[CostEstimationMessageContent]] = {
    MessageType.instance: InstanceContent,
    MessageType.program: ProgramContent,
    MessageType.store: StoreContent,
}


def parse_message(message_dict: Any) -> CostEstimationMessage:
    if not isinstance(message_dict, dict):
        raise InvalidMessageFormat("Message is not a dictionary")

    raw_message_type = message_dict.get("type")
    try:
        message_type = MessageType(raw_message_type)
    except ValueError as e:
        raise InvalidMessageFormat(f"Invalid message_type: '{raw_message_type}'") from e

    msg_cls = COST_MESSAGE_TYPE_TO_CLASS[message_type]

    try:
        return msg_cls(**message_dict)
    except ValidationError as e:
        raise InvalidMessageFormat(e.errors()) from e


async def validate_cost_estimation_message_content(message: CostEstimationMessage, storage_service: StorageService) -> CostEstimationMessageContent:
    content = await storage_service.get_message_content(message)
    content_type = COST_MESSAGE_TYPE_TO_CONTENT[message.type]
    return content_type.parse_obj(content.value)


def validate_cost_estimation_message_dict(message_dict: Any) -> CostEstimationMessage:
    try:
        return parse_message(message_dict)
    except InvalidMessageException as e:
        raise web.HTTPUnprocessableEntity(text=str(e))