from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship, Mapped

from .base import Base
from .chains import ChainTxDb


class PendingTxDb(Base):
    __tablename__ = "pending_txs"

    tx_hash: Mapped[str] = Column(ForeignKey("chain_txs.hash"), primary_key=True)

    tx: Mapped["ChainTxDb"] = relationship("ChainTxDb")
