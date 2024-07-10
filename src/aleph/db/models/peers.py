import datetime as dt
from enum import Enum

from sqlalchemy import TIMESTAMP, Column, String
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class PeerType(str, Enum):
    HTTP = "HTTP"
    IPFS = "IPFS"
    P2P = "P2P"


class PeerDb(Base):
    __tablename__ = "peers"

    peer_id = Column(String, primary_key=True)
    peer_type: PeerType = Column(ChoiceType(PeerType), primary_key=True)
    address = Column(String, nullable=False)
    source: PeerType = Column(ChoiceType(PeerType), nullable=False)
    last_seen: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
