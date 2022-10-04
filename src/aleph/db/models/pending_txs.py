from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

from .base import Base
from .chains import ChainTxDb


class PendingTxDb(Base):
    __tablename__ = "pending_txs"

    tx_hash: str = Column(ForeignKey("chain_txs.hash"), primary_key=True)

    tx: "ChainTxDb" = relationship("ChainTxDb")
