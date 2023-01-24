from enum import Enum
from typing import Optional, List, Any, Dict

from sqlalchemy import BigInteger, Column, String, ForeignKey, TIMESTAMP, Index
from sqlalchemy.orm import relationship
from sqlalchemy_utils import ChoiceType

from aleph.types.files import FileType
from .base import Base
import datetime as dt

from aleph.types.files import FileTag


class FilePinType(str, Enum):
    TX = "tx"
    MESSAGE = "message"


class StoredFileDb(Base):
    __tablename__ = "files"

    # id: int = Column(BigInteger, primary_key=True)

    hash: str = Column(String, nullable=False, primary_key=True)

    # TODO: compute hash equivalences
    # TODO: unique index for sha256
    # TODO: size constraints for hash fields
    # sha256_hex: Optional[str] = Column(String, nullable=True, index=True)
    # cidv0: str = Column(String, nullable=False, unique=True, index=True)
    # cidv1: str = Column(String, nullable=False, unique=True, index=True)

    # size: int = Column(BigInteger, nullable=False)
    # TODO: compute the size from local storage
    size: Optional[int] = Column(BigInteger, nullable=True)
    type: FileType = Column(ChoiceType(FileType), nullable=False)

    pins: List["FilePinDb"] = relationship("FilePinDb", back_populates="file")
    tags: List["FileTagDb"] = relationship("FileTagDb", back_populates="file")


class FileTagDb(Base):
    __tablename__ = "file_tags"

    tag: FileTag = Column(String, primary_key=True)
    owner: str = Column(String, nullable=False)
    file_hash: str = Column(ForeignKey(StoredFileDb.hash), nullable=False, index=True)
    last_updated: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)

    file: StoredFileDb = relationship(StoredFileDb, back_populates="tags")


class FilePinDb(Base):
    __tablename__ = "file_pins"

    id: int = Column(BigInteger, primary_key=True)

    file_hash: str = Column(ForeignKey(StoredFileDb.hash), nullable=False)
    created: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    type: str = Column(String, nullable=False)

    file: StoredFileDb = relationship(StoredFileDb, back_populates="pins")

    __mapper_args__: Dict[str, Any] = {
        "polymorphic_on": type,
    }


class TxFilePinDb(FilePinDb):
    tx_hash = Column(String, nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": FilePinType.TX.value,
    }


class MessageFilePinDb(FilePinDb):
    owner = Column(String, nullable=True, index=True)
    item_hash = Column(String, nullable=True, unique=True)
    ref = Column(String, nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": FilePinType.MESSAGE.value,
    }


Index(
    "ix_file_pins_owner",
    MessageFilePinDb.owner,
    postgresql_where=MessageFilePinDb.owner.isnot(None),
)
