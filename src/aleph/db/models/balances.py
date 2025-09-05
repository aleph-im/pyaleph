import datetime as dt
from decimal import Decimal
from typing import Optional

from aleph_message.models import Chain
from sqlalchemy import (
    DECIMAL,
    TIMESTAMP,
    BigInteger,
    Column,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.sql import func
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
    last_update: dt.datetime = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "address", "chain", "dapp", name="balances_address_chain_dapp_uindex"
        ),
    )


class AlephCreditBalanceDb(Base):
    __tablename__ = "credit_balances"

    id: int = Column(BigInteger, autoincrement=True)

    address: str = Column(String, nullable=False, index=True)
    amount: int = Column(BigInteger, nullable=False)
    ratio: Optional[Decimal] = Column(DECIMAL, nullable=True)
    tx_hash: Optional[str] = Column(String, nullable=True)
    token: Optional[str] = Column(String, nullable=True)
    chain: Optional[str] = Column(String, nullable=True)
    provider: Optional[str] = Column(String, nullable=True)
    origin: Optional[str] = Column(String, nullable=True)
    payment_ref: Optional[str] = Column(String, nullable=True)
    payment_method: Optional[str] = Column(String, nullable=True)
    distribution_ref: str = Column(String, nullable=False, primary_key=True)
    distribution_index: int = Column(Integer, nullable=False, primary_key=True)
    expiration_date: Optional[dt.datetime] = Column(
        TIMESTAMP(timezone=True), nullable=True
    )
    last_update: dt.datetime = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("tx_hash", name="credit_balances_tx_hash_uindex"),
    )
