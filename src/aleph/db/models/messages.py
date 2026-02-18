import datetime as dt
from typing import Any, Dict, List, Mapping, Optional, Type

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
)
from pydantic import ValidationError
from sqlalchemy import (
    ARRAY,
    TIMESTAMP,
    BigInteger,
    Column,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.channel import Channel
from aleph.types.message_status import ErrorCode, MessageStatus

from .base import Base
from .chains import ChainTxDb
from .pending_messages import PendingMessageDb

CONTENT_TYPE_MAP: Dict[MessageType, Type[BaseContent]] = {
    MessageType.aggregate: AggregateContent,
    MessageType.forget: ForgetContent,
    MessageType.instance: InstanceContent,
    MessageType.post: PostContent,
    MessageType.program: ProgramContent,
    MessageType.store: StoreContent,
}


message_confirmations = Table(
    "message_confirmations",
    Base.metadata,
    Column("id", Integer, primary_key=True),
    Column("item_hash", ForeignKey("messages.item_hash"), nullable=False, index=True),
    Column("tx_hash", ForeignKey("chain_txs.hash", ondelete="CASCADE"), nullable=False),
    UniqueConstraint("item_hash", "tx_hash"),
)


def validate_message_content(
    message_type: MessageType,
    content_dict: Dict[str, Any],
) -> BaseContent:
    content_type = CONTENT_TYPE_MAP[message_type]
    content = content_type.model_validate(content_dict)
    # Validate that the content time can be converted to datetime. This will
    # raise a ValueError and be caught
    # TODO: move this validation in aleph-message
    try:
        _ = dt.datetime.fromtimestamp(content_dict["time"])
    except ValueError as e:
        raise ValidationError(str(e)) from e

    return content


class MessageStatusDb(Base):
    __tablename__ = "message_status"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    status: Mapped[MessageStatus] = mapped_column(
        ChoiceType(MessageStatus), nullable=False
    )
    reception_time: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


class MessageDb(Base):
    """
    A message that was processed and validated by the CCN.
    """

    __tablename__ = "messages"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    type: Mapped[MessageType] = mapped_column(ChoiceType(MessageType), nullable=False)
    chain: Mapped[Chain] = mapped_column(ChoiceType(Chain), nullable=False)
    sender: Mapped[str] = mapped_column(String, nullable=False, index=True)
    signature: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    item_type: Mapped[ItemType] = mapped_column(ChoiceType(ItemType), nullable=False)
    item_content: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    content: Mapped[Any] = mapped_column(JSONB, nullable=False)
    time: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, index=True
    )
    channel: Mapped[Optional[Channel]] = mapped_column(
        String, nullable=True, index=True
    )
    size: Mapped[int] = mapped_column(Integer, nullable=False)

    # Denormalized columns (merged from message_status + JSONB content)
    status_value: Mapped[MessageStatus] = mapped_column(
        "status",
        ChoiceType(MessageStatus),
        nullable=False,
        default=MessageStatus.PROCESSED,
    )
    reception_time: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=utc_now,
    )
    owner: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    content_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    content_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    first_confirmed_at: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    first_confirmed_height: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True
    )
    payment_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    confirmations: Mapped[List[ChainTxDb]] = relationship(
        "ChainTxDb", secondary=message_confirmations
    )

    # Legacy relationship to MessageStatusDb (kept during transition, removed in Phase 7)
    status_rel: Mapped[Optional["MessageStatusDb"]] = relationship(
        "MessageStatusDb",
        primaryjoin="MessageDb.item_hash == MessageStatusDb.item_hash",
        foreign_keys=MessageStatusDb.item_hash,
        uselist=False,
    )

    def __init__(self, **kwargs) -> None:
        # Apply defaults for required denormalized columns
        kwargs.setdefault("status_value", MessageStatus.PROCESSED)
        kwargs.setdefault("reception_time", utc_now())

        # Auto-populate denormalized columns from content JSONB if not explicitly set
        content = kwargs.get("content")
        if isinstance(content, dict):
            kwargs.setdefault("owner", content.get("address"))
            kwargs.setdefault("content_type", content.get("type"))
            kwargs.setdefault("content_ref", content.get("ref"))
            kwargs.setdefault("content_key", content.get("key"))
        super().__init__(**kwargs)
        self._parsed_content: Optional[BaseContent] = None

    @property
    def confirmed(self) -> bool:
        return bool(self.confirmations)

    @property
    def parsed_content(self):
        if getattr(self, "_parsed_content", None) is None:
            self._parsed_content = validate_message_content(self.type, self.content)
        return self._parsed_content

    @staticmethod
    def _coerce_content(
        pending_message: PendingMessageDb, content_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        if content_dict.get("address") is None:
            content_dict["address"] = pending_message.sender
        if content_dict.get("time") is None:
            content_dict["time"] = pending_message.time.timestamp()
        return content_dict

    @classmethod
    def from_pending_message(
        cls,
        pending_message: PendingMessageDb,
        content_dict: Dict[str, Any],
        content_size: int,
        reception_time: Optional[dt.datetime] = None,
    ) -> "MessageDb":
        if reception_time is None:
            reception_time = pending_message.reception_time
        content_dict = cls._coerce_content(pending_message, content_dict)
        parsed_content = validate_message_content(pending_message.type, content_dict)

        message = cls(
            item_hash=pending_message.item_hash,
            type=pending_message.type,
            chain=pending_message.chain,
            sender=pending_message.sender,
            signature=pending_message.signature,
            item_type=pending_message.item_type,
            item_content=pending_message.item_content,
            content=content_dict,
            time=pending_message.time,
            channel=pending_message.channel,
            size=content_size,
            # Denormalized columns
            status_value=MessageStatus.PROCESSED,
            reception_time=reception_time,
            owner=content_dict.get("address"),
            content_type=content_dict.get("type"),
            content_ref=content_dict.get("ref"),
            content_key=content_dict.get("key"),
        )
        message._parsed_content = parsed_content
        return message

    @classmethod
    def from_message_dict(cls, message_dict: Dict[str, Any]) -> "MessageDb":
        """
        Utility function to translate Aleph message dictionaries, such as those returned by the API,
        in the corresponding DB object.
        """

        item_hash = message_dict["item_hash"]

        return cls(
            item_hash=item_hash,
            type=message_dict["type"],
            chain=Chain(message_dict["chain"]),
            sender=message_dict["sender"],
            signature=message_dict["signature"],
            item_type=ItemType(message_dict.get("item_type", ItemType.inline)),
            item_content=message_dict.get("item_content"),
            content=message_dict["content"],
            time=timestamp_to_datetime(message_dict["time"]),
            channel=message_dict.get("channel"),
            size=message_dict.get("size", 0),
        )


# TODO: move these to their own files?
class ForgottenMessageDb(Base):
    __tablename__ = "forgotten_messages"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    type: Mapped[MessageType] = mapped_column(ChoiceType(MessageType), nullable=False)
    chain: Mapped[Chain] = mapped_column(ChoiceType(Chain), nullable=False)
    sender: Mapped[str] = mapped_column(String, nullable=False, index=True)
    signature: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    item_type: Mapped[ItemType] = mapped_column(ChoiceType(ItemType), nullable=False)
    time: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, index=True
    )
    channel: Mapped[Optional[Channel]] = mapped_column(
        String, nullable=True, index=True
    )
    forgotten_by: Mapped[List[str]] = mapped_column(ARRAY(String), nullable=False)


class ErrorCodeDb(Base):
    __tablename__ = "error_codes"

    code: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(String, nullable=False)


class RejectedMessageDb(Base):
    __tablename__ = "rejected_messages"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    message: Mapped[Mapping[str, Any]] = mapped_column(JSONB, nullable=False)
    error_code: Mapped[ErrorCode] = mapped_column(
        ChoiceType(ErrorCode, impl=Integer()), nullable=False
    )
    details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    traceback: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tx_hash: Mapped[Optional[str]] = mapped_column(
        ForeignKey("chain_txs.hash"), nullable=True
    )
