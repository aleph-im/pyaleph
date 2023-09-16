from decimal import Decimal
from typing import Optional

from aleph_message.models import Chain
from sqlalchemy import Column, DECIMAL, String, Integer, UniqueConstraint, BigInteger
from sqlalchemy.orm import Mapped
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class AlephBalanceDb(Base):
    __tablename__ = "balances"

    id: Mapped[int] = Column(BigInteger, primary_key=True)

    address: Mapped[str] = Column(String, nullable=False, index=True)
    chain: Mapped[Chain] = Column(ChoiceType(Chain), nullable=False)
    dapp: Mapped[Optional[str]] = Column(String, nullable=True)
    eth_height: Mapped[int] = Column(Integer, nullable=False)
    balance: Mapped[Decimal] = Column(DECIMAL, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "address", "chain", "dapp", name="balances_address_chain_dapp_uindex"
        ),
    )
