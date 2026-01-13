from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .chains import ChainTxDb


class PendingTxDb(Base):
    __tablename__ = "pending_txs"

    tx_hash: Mapped[str] = mapped_column(ForeignKey("chain_txs.hash"), primary_key=True)

    tx: Mapped["ChainTxDb"] = relationship("ChainTxDb")
