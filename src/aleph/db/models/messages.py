import datetime as dt
from typing import Any, ClassVar, Dict, FrozenSet, List, Mapping, Optional, Type

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
    TIMESTAMP,
    BigInteger,
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
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


def extract_tags(
    message_type: Any, content_dict: Mapping[str, Any]
) -> Optional[List[str]]:
    """Pull the tag list out of a content payload.

    Tags live in different keys depending on the message type:

    * POST + AGGREGATE: ``content -> 'content' -> 'tags'``
    * STORE:            ``content -> 'tags'``
    * INSTANCE/PROGRAM: ``content -> 'metadata' -> 'tags'``

    Returns ``None`` when the message carries no tags so the caller can
    leave the column NULL, distinguishable from an explicitly empty list.
    """
    if not isinstance(message_type, MessageType):
        try:
            message_type = MessageType(message_type)
        except (ValueError, KeyError):
            return None

    if message_type in (MessageType.post, MessageType.aggregate):
        inner = content_dict.get("content")
        tags = inner.get("tags") if isinstance(inner, dict) else None
    elif message_type == MessageType.store:
        tags = content_dict.get("tags")
    elif message_type in (MessageType.instance, MessageType.program):
        metadata = content_dict.get("metadata")
        tags = metadata.get("tags") if isinstance(metadata, dict) else None
    else:
        tags = None

    if not isinstance(tags, list) or not tags:
        return None
    return [t for t in tags if isinstance(t, str)] or None


message_confirmations = Table(
    "message_confirmations",
    Base.metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "item_hash",
        ForeignKey("messages.item_hash", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
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

    # Column names that exist on the table for indexing/joining but are NOT
    # part of the canonical aleph-message wire format. API responses and any
    # ``MessageDb.to_dict()`` payload bound for an aleph-message validator
    # must strip these — pydantic models like ``PostMessage`` use
    # ``extra="forbid"`` and will reject a stray denormalized field.
    DENORMALIZED_COLUMNS: ClassVar[FrozenSet[str]] = frozenset(
        {
            "status",
            "reception_time",
            "owner",
            "content_type",
            "content_ref",
            "content_key",
            "content_item_hash",
            "first_confirmed_at",
            "first_confirmed_height",
            "payment_type",
            "tags",
        }
    )

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    type: Mapped[MessageType] = mapped_column(ChoiceType(MessageType), nullable=False)
    chain: Mapped[Chain] = mapped_column(ChoiceType(Chain), nullable=False)
    sender: Mapped[str] = mapped_column(String, nullable=False, index=True)
    signature: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    item_type: Mapped[ItemType] = mapped_column(ChoiceType(ItemType), nullable=False)
    item_content: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    content: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
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
    content_item_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tags: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String), nullable=True)

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
            kwargs.setdefault("content_item_hash", content.get("item_hash"))
            # Derive payment_type from content.payment.type if present
            payment = content.get("payment")
            if isinstance(payment, dict) and payment.get("type"):
                kwargs.setdefault("payment_type", payment["type"])
            message_type = kwargs.get("type")
            if message_type is not None:
                kwargs.setdefault("tags", extract_tags(message_type, content))
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

        # Derive payment_type from parsed content for types that support it
        payment_type = None
        payment = getattr(parsed_content, "payment", None)
        if payment is not None:
            if payment.is_credit:
                payment_type = "credit"
            elif payment.is_stream:
                payment_type = "superfluid"
            else:
                payment_type = "hold"

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
            content_item_hash=content_dict.get("item_hash"),
            payment_type=payment_type,
            tags=extract_tags(pending_message.type, content_dict),
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
    # Billing metadata preserved at forget time. NULL for rows forgotten
    # before these columns existed (legacy rows).
    owner: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    payment_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    size: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    # Sender-supplied time of the forgetting FORGET message. Like the live
    # list's default time sort/cursor, windowed consumers assume the gap a
    # backdated FORGET can introduce.
    forgotten_at: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    __table_args__ = (
        # The forgotten list endpoint filters by owner and windows/sorts on
        # forgotten_at (deletion time).
        Index(
            "ix_forgotten_messages_owner_forgotten_at",
            "owner",
            "forgotten_at",
        ),
        Index("ix_forgotten_messages_forgotten_at", "forgotten_at"),
    )


class RemovedMessageDb(Base):
    """
    Snapshot of a message removed by the balance/credit-balance cron jobs,
    mirroring forgotten_messages: at REMOVING->REMOVED the messages row is
    deleted and this snapshot becomes the only record of the message.

    Two-phase lifecycle: the file size is snapshotted at PROCESSED->REMOVING
    (while the files row still exists — the garbage collector deletes it
    before the status flips to REMOVED), the row is deleted on
    REMOVING->PROCESSED recovery, and the remaining metadata is copied from
    the messages row and stamped with removed_at by the garbage collector at
    REMOVING->REMOVED, right before the messages row is deleted.
    """

    __tablename__ = "removed_messages"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    # Metadata copied from the messages row at REMOVING->REMOVED (the row is
    # deleted right after). NULL while the message is still REMOVING — the
    # messages row is the source of truth until the flip.
    type: Mapped[Optional[MessageType]] = mapped_column(
        ChoiceType(MessageType), nullable=True
    )
    chain: Mapped[Optional[Chain]] = mapped_column(ChoiceType(Chain), nullable=True)
    sender: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    signature: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    item_type: Mapped[Optional[ItemType]] = mapped_column(
        ChoiceType(ItemType), nullable=True
    )
    time: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    channel: Mapped[Optional[Channel]] = mapped_column(String, nullable=True)
    owner: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # Effective payment type (NULL payment coalesced to hold at copy time).
    payment_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # files.size snapshot taken at PROCESSED->REMOVING while the message was
    # alive (NULL for non-STORE messages or when the size could not be
    # resolved).
    size: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    # Stamped by the garbage collector at REMOVING->REMOVED. Node-local and
    # NOT deterministic across nodes: each node's GC finalizes removals on
    # its own schedule, and — unlike forgotten_at — there is no
    # sender-declared removal time to share (removal is a node-local balance
    # decision). Consumers windowing on it must not expect two nodes to
    # agree. NULL while the message is still REMOVING and for legacy rows
    # (removed before this table recorded removal times).
    removed_at: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    __table_args__ = (
        # The removed list endpoint filters by owner and windows/sorts on
        # removed_at.
        Index("ix_removed_messages_owner_removed_at", "owner", "removed_at"),
        Index("ix_removed_messages_removed_at", "removed_at"),
    )


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
