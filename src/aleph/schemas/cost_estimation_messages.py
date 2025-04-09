from typing import Any, Dict, Generic, List, Literal, Optional, Type, TypeAlias, Union

from aiohttp import web
from aleph_message.models import (
    InstanceContent,
    MessageType,
    ProgramContent,
    StoreContent,
)
from aleph_message.models.execution.program import (
    CodeContent,
    DataContent,
    FunctionRuntime,
)
from aleph_message.models.execution.volume import (
    EphemeralVolume,
    ImmutableVolume,
    PersistentVolume,
)
from pydantic import Field, ValidationError, model_validator

from aleph.schemas.base_messages import AlephBaseMessage, ContentType, MType
from aleph.schemas.pending_messages import base_pending_message_load_content
from aleph.storage import StorageService
from aleph.types.message_status import InvalidMessageException, InvalidMessageFormat


class CostEstimationImmutableVolume(ImmutableVolume):
    estimated_size_mib: Optional[int] = None


CostEstimationMachineVolume = Union[
    CostEstimationImmutableVolume,
    EphemeralVolume,
    PersistentVolume,
]


class CostEstimationInstanceContent(InstanceContent):
    volumes: List[CostEstimationMachineVolume] = Field(
        default=[], description="Volumes to mount on the filesystem"
    )


class CostEstimationCodeContent(CodeContent):
    estimated_size_mib: Optional[int] = None


class CostEstimationFunctionRuntime(FunctionRuntime):
    estimated_size_mib: Optional[int] = None


class CostEstimationDataContent(DataContent):
    estimated_size_mib: Optional[int] = None


class CostEstimationProgramContent(ProgramContent):
    code: CostEstimationCodeContent = Field(description="Code to execute")
    runtime: CostEstimationFunctionRuntime = Field(
        description="Execution runtime (rootfs with Python interpreter)"
    )
    data: Optional[CostEstimationDataContent] = Field(
        default=None, description="Data to use during computation"
    )
    volumes: List[CostEstimationMachineVolume] = Field(
        default=[], description="Volumes to mount on the filesystem"
    )


class CostEstimationStoreContent(StoreContent):
    estimated_size_mib: Optional[int] = None


class BaseCostEstimationMessage(AlephBaseMessage, Generic[MType, ContentType]):
    """
    A raw Aleph message, sent by users to estimate costs before reaching the network
    """

    type: MType
    item_hash: str

    @model_validator(mode="before")
    def load_content(cls, values):
        return base_pending_message_load_content(values)


class CostEstimationInstanceMessage(
    BaseCostEstimationMessage[Literal[MessageType.instance], CostEstimationInstanceContent]  # type: ignore
):
    pass


class CostEstimationProgramMessage(
    BaseCostEstimationMessage[Literal[MessageType.program], CostEstimationProgramContent]  # type: ignore
):
    pass


class CostEstimationStoreMessage(BaseCostEstimationMessage[Literal[MessageType.store], CostEstimationStoreContent]):  # type: ignore
    pass


CostEstimationMessage: TypeAlias = (
    CostEstimationInstanceMessage
    | CostEstimationProgramMessage
    | CostEstimationStoreMessage
)


CostEstimationContent: TypeAlias = (
    CostEstimationInstanceContent
    | CostEstimationProgramContent
    | CostEstimationStoreContent
)


CostEstimationExecutableContent: TypeAlias = (
    CostEstimationInstanceContent | CostEstimationProgramContent
)
CostEstimationExecutableMessage: TypeAlias = (
    CostEstimationInstanceMessage | CostEstimationProgramMessage
)


COST_MESSAGE_TYPE_TO_CLASS: Dict[
    MessageType,
    Type[CostEstimationMessage],
] = {
    MessageType.instance: CostEstimationInstanceMessage,
    MessageType.program: CostEstimationProgramMessage,
    MessageType.store: CostEstimationStoreMessage,
}


COST_MESSAGE_TYPE_TO_CONTENT: Dict[MessageType, Type[CostEstimationContent]] = {
    MessageType.instance: CostEstimationInstanceContent,
    MessageType.program: CostEstimationProgramContent,
    MessageType.store: CostEstimationStoreContent,
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


def validate_cost_estimation_message_dict(message_dict: Any) -> CostEstimationMessage:
    try:
        return parse_message(message_dict)
    except InvalidMessageException as e:
        raise web.HTTPUnprocessableEntity(text=str(e))


async def validate_cost_estimation_message_content(
    message: CostEstimationMessage, storage_service: StorageService
) -> CostEstimationContent:
    content = await storage_service.get_message_content(message)
    content_type = COST_MESSAGE_TYPE_TO_CONTENT[message.type]
    return content_type.model_validate(content.value)
