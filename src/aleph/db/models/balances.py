import datetime as dt
from decimal import Decimal
from typing import Optional

from aleph_message.models import Chain
from sqlalchemy import DECIMAL, TIMESTAMP, BigInteger, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class AlephBalanceDb(Base):
    __tablename__ = "balances"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    address: Mapped[str] = mapped_column(String, nullable=False, index=True)
    chain: Mapped[Chain] = mapped_column(ChoiceType(Chain), nullable=False)
    dapp: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    eth_height: Mapped[int] = mapped_column(Integer, nullable=False)
    balance: Mapped[Decimal] = mapped_column(DECIMAL, nullable=False)
    last_update: Mapped[dt.datetime] = mapped_column(
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


class AlephCreditHistoryDb(Base):
    __tablename__ = "credit_history"

    id: Mapped[int] = mapped_column(BigInteger, autoincrement=True)

    address: Mapped[str] = mapped_column(String, nullable=False, index=True)
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    price: Mapped[Optional[Decimal]] = mapped_column(DECIMAL, nullable=True)
    bonus_amount: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    tx_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    token: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    chain: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    provider: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    origin: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    origin_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    payment_method: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    credit_ref: Mapped[str] = mapped_column(String, nullable=False, primary_key=True)
    credit_index: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    expiration_date: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    message_timestamp: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        index=True,
    )
    last_update: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class AlephCreditBalanceDb(Base):
    __tablename__ = "credit_balances"

    address: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    balance: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_update: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
