import datetime as dt
from enum import Enum
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType

from aleph.types.files import FileTag, FileType

from .base import Base


class FilePinType(str, Enum):
    # The file containing the content field of a non-inline message.
    CONTENT = "content"
    # A file pinned by a message, ex: STORE message.
    MESSAGE = "message"
    # A file containing sync messages.
    TX = "tx"
    # A file with a grace period (=no one paying for the file, but we keep it around for a while).
    GRACE_PERIOD = "grace_period"


class StoredFileDb(Base):
    __tablename__ = "files"

    # id: int = Column(BigInteger, primary_key=True)

    hash: Mapped[str] = mapped_column(String, nullable=False, primary_key=True)

    # TODO: compute hash equivalences
    # TODO: unique index for sha256
    # TODO: size constraints for hash fields
    # sha256_hex: Optional[str] = Column(String, nullable=True, index=True)
    # cidv0: str = Column(String, nullable=False, unique=True, index=True)
    # cidv1: str = Column(String, nullable=False, unique=True, index=True)

    # size: int = Column(BigInteger, nullable=False)
    # TODO: compute the size from local storage
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    type: Mapped[FileType] = mapped_column(ChoiceType(FileType), nullable=False)

    pins: Mapped[List["FilePinDb"]] = relationship("FilePinDb", back_populates="file")
    tags: Mapped[List["FileTagDb"]] = relationship("FileTagDb", back_populates="file")


class FileTagDb(Base):
    __tablename__ = "file_tags"

    tag: Mapped[FileTag] = mapped_column(String, primary_key=True)
    owner: Mapped[str] = mapped_column(String, nullable=False)
    file_hash: Mapped[str] = mapped_column(
        ForeignKey(StoredFileDb.hash), nullable=False, index=True
    )
    last_updated: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    file: Mapped[StoredFileDb] = relationship(StoredFileDb, back_populates="tags")


class FilePinDb(Base):
    __tablename__ = "file_pins"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    file_hash: Mapped[str] = mapped_column(
        ForeignKey(StoredFileDb.hash), nullable=False
    )
    created: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    type: Mapped[str] = mapped_column(String, nullable=False)
    # TODO: these columns should be defined on Message/ContentFilePinDb instead with `use_existing`.
    #       This field is only available since SQLA 2.0.
    owner: Mapped[str] = mapped_column(String, nullable=True, index=True)
    item_hash: Mapped[str] = mapped_column(String, nullable=True)

    # Allow to recover MESSAGE pins refs marked for removing from grace period entries
    ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    file: Mapped[StoredFileDb] = relationship(StoredFileDb, back_populates="pins")

    __mapper_args__: Dict[str, Any] = {
        "polymorphic_on": type,
        "polymorphic_identity": "file_pin",
    }
    __table_args__ = (UniqueConstraint("item_hash", "type"),)


class TxFilePinDb(FilePinDb):
    tx_hash: Mapped[str] = mapped_column(String, nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": FilePinType.TX.value,
    }


class MessageFilePinDb(FilePinDb):
    # ref = Column(String, nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": FilePinType.MESSAGE.value,
    }


class ContentFilePinDb(FilePinDb):

    __mapper_args__ = {
        "polymorphic_identity": FilePinType.CONTENT.value,
    }


class GracePeriodFilePinDb(FilePinDb):
    delete_by: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    __mapper_args__ = {
        "polymorphic_identity": FilePinType.GRACE_PERIOD.value,
    }


Index(
    "ix_file_pins_owner",
    MessageFilePinDb.owner,
    postgresql_where=MessageFilePinDb.owner.isnot(None),
)


Index(
    "ix_file_pins_delete_by",
    GracePeriodFilePinDb.delete_by,
    postgresql_where=GracePeriodFilePinDb.delete_by.isnot(None),
)
