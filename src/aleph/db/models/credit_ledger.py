import datetime as dt
from decimal import Decimal
from typing import Any, Dict, Optional

from aleph_message.models import MessageType, PaymentType
from sqlalchemy import (
    DECIMAL,
    NUMERIC,
    TIMESTAMP,
    BigInteger,
    Boolean,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy_utils.types.choice import ChoiceType

from .base import Base


class BillingResourceDb(Base):
    __tablename__ = "billing_resources"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    type: Mapped[MessageType] = mapped_column(ChoiceType(MessageType), nullable=False)
    owner: Mapped[str] = mapped_column(String, nullable=False)
    payment_type: Mapped[PaymentType] = mapped_column(
        ChoiceType(PaymentType), nullable=False
    )
    first_confirmed_at: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    end_of_life: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    end_of_life_provisional: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    end_reason: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    cost_spec: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_billing_resources_owner_payment_type", "owner", "payment_type"),
        Index(
            "ix_billing_resources_type_payment_type_first_confirmed_at",
            "type",
            "payment_type",
            "first_confirmed_at",
        ),
        Index("ix_billing_resources_end_of_life", "end_of_life"),
    )


class BillingRateDb(Base):
    __tablename__ = "billing_rates"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    pricing_version: Mapped[str] = mapped_column(String, primary_key=True)
    rate_credit: Mapped[Decimal] = mapped_column(DECIMAL(40, 18), nullable=False)


class CreditLedgerDb(Base):
    __tablename__ = "credit_ledger"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    period: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    address: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    kind: Mapped[str] = mapped_column(String, nullable=False)
    business_key: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    resource: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    node_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # type: ignore[assignment]
    size_mib: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    rate: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(40, 18), nullable=True)
    expiration: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    source_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    excluded_reason: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("kind", "business_key", name="uq_credit_ledger_kind_key"),
        Index("ix_credit_ledger_address_period", "address", "period"),
        Index("ix_credit_ledger_period_kind", "period", "kind"),
        Index("ix_credit_ledger_resource", "resource"),
        Index("ix_credit_ledger_node_id_period", "node_id", "period"),
    )


class CreditLotDb(Base):
    __tablename__ = "credit_lots"

    ledger_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("credit_ledger.id"), primary_key=True
    )
    address: Mapped[str] = mapped_column(String, nullable=False)
    period: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    kind: Mapped[str] = mapped_column(String, nullable=False)
    business_key: Mapped[str] = mapped_column(String, nullable=False)
    amount_remaining: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expiration: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    __table_args__ = (
        Index(
            "ix_credit_lots_address_order",
            "address",
            "period",
            "kind",
            "business_key",
        ),
        Index(
            "ix_credit_lots_address_active",
            "address",
            postgresql_where=text("amount_remaining > 0"),
        ),
    )


class CreditCheckpointDb(Base):
    __tablename__ = "credit_checkpoints"

    period: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True
    )
    # '' is the row holding the global hash.
    address: Mapped[str] = mapped_column(String, primary_key=True)
    lots: Mapped[Any] = mapped_column(JSONB, nullable=False)
    ledger_hash: Mapped[str] = mapped_column(String, nullable=False)


class CreditTopupDb(Base):
    __tablename__ = "credit_topups"

    chain_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tx_hash: Mapped[str] = mapped_column(String, primary_key=True)
    log_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    block_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    block_time: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    from_address: Mapped[str] = mapped_column(String, nullable=False)
    token: Mapped[str] = mapped_column(String, nullable=False)
    raw_amount: Mapped[Decimal] = mapped_column(NUMERIC(78, 0), nullable=False)
    quote_num: Mapped[Optional[Decimal]] = mapped_column(NUMERIC(78, 0), nullable=True)
    quote_den: Mapped[Optional[Decimal]] = mapped_column(NUMERIC(78, 0), nullable=True)
    credits: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    # credited|excluded_router|excluded_process_tx|excluded_denylist|unquoted|zero
    status: Mapped[str] = mapped_column(String, nullable=False)
    ledger_period: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )


class CreditAttestationDb(Base):
    __tablename__ = "credit_attestations"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    period: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    sender: Mapped[str] = mapped_column(String, nullable=False)
    supersedes: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # active|duplicate|superseded
    status: Mapped[str] = mapped_column(String, nullable=False)
    received_at: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index(
            "ux_credit_attestations_active_period",
            "period",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )


class CreditFoldStateDb(Base):
    __tablename__ = "credit_fold_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    last_closed_period: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    dirty_from_period: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    chain_cursor_block: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    chain_cursor_time: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    genesis_loaded_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    backfill_done: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )


class CreditPeriodQuoteDb(Base):
    __tablename__ = "credit_period_quotes"

    period: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True
    )
    aleph_per_credit: Mapped[Decimal] = mapped_column(NUMERIC(40, 18), nullable=False)
    block_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
