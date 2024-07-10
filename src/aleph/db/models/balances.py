from decimal import Decimal
from typing import Optional

from aleph_message.models import Chain
from sqlalchemy import DECIMAL, BigInteger, Column, Integer, String, UniqueConstraint
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class AlephBalanceDb(Base):
    __tablename__ = "balances"

    id: int = Column(BigInteger, primary_key=True)

    address: str = Column(String, nullable=False, index=True)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    dapp: Optional[str] = Column(String, nullable=True)
    eth_height: int = Column(Integer, nullable=False)
    balance: Decimal = Column(DECIMAL, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "address", "chain", "dapp", name="balances_address_chain_dapp_uindex"
        ),
    )
