import datetime as dt
from enum import Enum

from sqlalchemy import TIMESTAMP, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class PeerType(str, Enum):
    HTTP = "HTTP"
    IPFS = "IPFS"
    P2P = "P2P"


class PeerDb(Base):
    __tablename__ = "peers"

    peer_id: Mapped[str] = mapped_column(String, primary_key=True)
    peer_type: Mapped[PeerType] = mapped_column(ChoiceType(PeerType), primary_key=True)
    address: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[PeerType] = mapped_column(ChoiceType(PeerType), nullable=False)
    last_seen: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
