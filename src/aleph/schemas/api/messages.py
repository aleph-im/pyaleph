import datetime as dt
from typing import (
    Annotated,
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
)

from aleph_message.models import (
    AggregateContent,
    BaseContent,
    Chain,
    ForgetContent,
    InstanceContent,
    ItemType,
    MessageType,
    PostContent,
    ProgramContent,
    StoreContent,
    VerifiableProgramContent,
)
from pydantic import BaseModel, ConfigDict, Field, field_serializer

from aleph.db.models import MessageDb
from aleph.types.message_status import ErrorCode, MessageStatus, RemovedMessageReason

MType = TypeVar("MType", bound=MessageType)
ContentType = TypeVar("ContentType", bound=BaseContent)


class MessageConfirmation(BaseModel):
    """Format of the result when a message has been confirmed on a blockchain"""

    model_config = ConfigDict(from_attributes=True)

    chain: Chain
    height: int
    hash: str

    # Omit this field for now as are not exported with previous Pydantic version. TODO: Review if has to be added
    # datetime: dt.datetime
    #
    # @field_serializer("datetime")
    # def serialize_time(self, dt: dt.datetime, _info) -> float:
    #     return dt.timestamp()


class BaseMessage(BaseModel, Generic[MType, ContentType]):
    model_config = ConfigDict(from_attributes=True)

    sender: str
    chain: Chain
    signature: Optional[str] = None
    type: MType
    item_content: Optional[str] = None
    item_type: ItemType
    item_hash: str
    time: dt.datetime
    channel: Optional[str] = None
    content: ContentType
    confirmed: bool
    confirmations: List[MessageConfirmation]

    @field_serializer("time")
    def serialize_time(self, dt: dt.datetime, _info) -> float:
        return dt.timestamp()


class AggregateMessage(
    BaseMessage[Literal[MessageType.aggregate], AggregateContent]
): ...


class ForgetMessage(BaseMessage[Literal[MessageType.forget], ForgetContent]): ...


class InstanceMessage(BaseMessage[Literal[MessageType.instance], InstanceContent]): ...


class PostMessage(BaseMessage[Literal[MessageType.post], PostContent]): ...


class ProgramMessage(BaseMessage[Literal[MessageType.program], ProgramContent]): ...


class StoreMessage(BaseMessage[Literal[MessageType.store], StoreContent]): ...


class VProgramMessage(
    BaseMessage[Literal[MessageType.v_program], VerifiableProgramContent]
): ...


MESSAGE_CLS_DICT: Dict[
    Any,
    Type[
        AggregateMessage
        | ForgetMessage
        | InstanceMessage
        | PostMessage
        | ProgramMessage
        | StoreMessage
        | VProgramMessage
    ],
] = {
    MessageType.aggregate: AggregateMessage,
    MessageType.forget: ForgetMessage,
    MessageType.instance: InstanceMessage,
    MessageType.post: PostMessage,
    MessageType.program: ProgramMessage,
    MessageType.store: StoreMessage,
    MessageType.v_program: VProgramMessage,
}


AlephMessage = Annotated[
    Union[
        AggregateMessage,
        ForgetMessage,
        InstanceMessage,
        PostMessage,
        ProgramMessage,
        StoreMessage,
        VProgramMessage,
    ],
    Field(discriminator="type"),
]


def format_message(message: MessageDb) -> AlephMessage:
    message_type = message.type

    message_cls = MESSAGE_CLS_DICT[message_type]

    return message_cls.model_validate(message)


def format_message_dict(message: Dict[str, Any]) -> AlephMessage:
    message_type = message.get("type")
    message_cls = MESSAGE_CLS_DICT[message_type]
    return message_cls.model_validate(message)


class BaseMessageStatus(BaseModel):
    status: MessageStatus
    item_hash: str
    reception_time: dt.datetime


# We already have a model for the validation of pending messages, but this one
# is only used for formatting and does not try to be smart.
class PendingMessage(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sender: str
    chain: Chain
    signature: Optional[str] = None
    type: MessageType
    item_content: Optional[str] = None
    item_type: ItemType
    item_hash: str
    time: dt.datetime
    channel: Optional[str] = None
    content: Optional[Dict[str, Any]] = None
    reception_time: dt.datetime


class PendingMessageStatus(BaseMessageStatus):
    model_config = ConfigDict(from_attributes=True)

    status: MessageStatus = MessageStatus.PENDING
    messages: List[PendingMessage]


class ProcessedMessageStatus(BaseMessageStatus):
    model_config = ConfigDict(from_attributes=True)

    status: MessageStatus = MessageStatus.PROCESSED
    message: AlephMessage


class RemovingMessageStatus(BaseMessageStatus):
    model_config = ConfigDict(from_attributes=True)

    status: MessageStatus = MessageStatus.REMOVING
    message: AlephMessage
    reason: RemovedMessageReason


class RemovedMessage(BaseModel):
    """
    Skeleton of a removed message, rebuilt from the removed_messages
    snapshot: the messages row is deleted at removal, mirroring forgotten
    messages. Metadata fields are NULL for legacy rows (removed before the
    snapshot existed).
    """

    model_config = ConfigDict(from_attributes=True)

    sender: Optional[str] = None
    chain: Optional[Chain] = None
    signature: Optional[str] = None
    type: Optional[MessageType] = None
    item_type: Optional[ItemType] = None
    item_hash: str
    time: Optional[dt.datetime] = None
    channel: Optional[str] = None
    # Billing metadata preserved at removal.
    owner: Optional[str] = None
    payment_type: Optional[str] = None
    size: Optional[int] = None
    # Node-local removal finalization time — NOT deterministic across nodes
    # (each node's GC finalizes removals on its own schedule); the removed
    # list windows and sorts on it.
    removed_at: Optional[dt.datetime] = None

    @field_serializer("time", "removed_at")
    def serialize_optional_datetime(
        self, value: Optional[dt.datetime], _info
    ) -> Optional[float]:
        return value.timestamp() if value is not None else None


class RemovedMessageStatus(BaseMessageStatus):
    model_config = ConfigDict(from_attributes=True)

    status: MessageStatus = MessageStatus.REMOVED
    message: RemovedMessage
    reason: RemovedMessageReason
    # Removal record: file-size snapshot taken at PROCESSED->REMOVING and
    # removal time stamped at REMOVING->REMOVED (NULL for legacy removals).
    removed_at: Optional[dt.datetime] = None
    size: Optional[int] = None

    @field_serializer("removed_at")
    def serialize_removed_at(
        self, value: Optional[dt.datetime], _info
    ) -> Optional[float]:
        return value.timestamp() if value is not None else None


class ForgottenMessage(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sender: str
    chain: Chain
    signature: Optional[str] = None
    type: MessageType
    item_type: ItemType
    item_hash: str
    time: dt.datetime
    channel: Optional[str] = None
    # Billing metadata preserved at forget time (NULL for legacy rows).
    owner: Optional[str] = None
    payment_type: Optional[str] = None
    size: Optional[int] = None
    # Sender-supplied time of the forgetting FORGET message; the forgotten
    # list windows and sorts on it.
    forgotten_at: Optional[dt.datetime] = None

    @field_serializer("time")
    def serialize_time(self, dt: dt.datetime, _info) -> float:
        return dt.timestamp()

    @field_serializer("forgotten_at")
    def serialize_forgotten_at(
        self, value: Optional[dt.datetime], _info
    ) -> Optional[float]:
        return value.timestamp() if value is not None else None


class ForgottenMessageStatus(BaseMessageStatus):
    status: MessageStatus = MessageStatus.FORGOTTEN
    message: ForgottenMessage
    forgotten_by: List[str]


class RejectedMessageStatus(BaseMessageStatus):
    status: MessageStatus = MessageStatus.REJECTED
    message: Mapping[str, Any]
    error_code: ErrorCode
    details: Any = None


class MessageStatusInfo(BaseMessageStatus):
    model_config = ConfigDict(from_attributes=True)


class MessageHashes(BaseMessageStatus):
    model_config = ConfigDict(from_attributes=True)


MessageWithStatus = Union[
    PendingMessageStatus,
    ProcessedMessageStatus,
    ForgottenMessageStatus,
    RejectedMessageStatus,
    RemovingMessageStatus,
    RemovedMessageStatus,
]


class MessageListResponse(BaseModel):
    messages: List[AlephMessage]
    pagination_page: int
    pagination_total: int
    pagination_per_page: int
    pagination_item: Literal["messages"] = "messages"
    time: dt.datetime

    @field_serializer("time")
    def serialize_time(self, dt: dt.datetime, _info) -> float:
        return dt.timestamp()
