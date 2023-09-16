import datetime as dt
from enum import Enum

from sqlalchemy import Column, String, TIMESTAMP
from sqlalchemy.orm import Mapped
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class PeerType(str, Enum):
    HTTP = "HTTP"
    IPFS = "IPFS"
    P2P = "P2P"


class PeerDb(Base):
    __tablename__ = "peers"

    peer_id: Mapped[str] = Column(String, primary_key=True)
    peer_type: Mapped[PeerType] = Column(ChoiceType(PeerType), primary_key=True)
    address: Mapped[str] = Column(String, nullable=False)
    source: Mapped[PeerType] = Column(ChoiceType(PeerType), nullable=False)
    last_seen: Mapped[dt.datetime] = Column(TIMESTAMP(timezone=True), nullable=False)
