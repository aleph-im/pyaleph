import datetime as dt
from typing import Optional

from sqlalchemy import TIMESTAMP, BigInteger, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

# crn_metrics and ccn_metrics are RANGE-partitioned on measured_at at the
# Postgres level. Partition DDL lives in the alembic migration; SQLAlchemy
# only sees the logical parent table. The PK is composite (id, measured_at)
# because Postgres requires the partition key to be part of any PK on a
# partitioned table; id is still a BIGSERIAL sequence column.


class CrnMetricDb(Base):
    __tablename__ = "crn_metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    measured_at: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True, nullable=False
    )
    item_hash: Mapped[str] = mapped_column(
        ForeignKey("messages.item_hash", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    node_id: Mapped[str] = mapped_column(String, nullable=False)
    base_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    base_latency_ipv4: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    full_check_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    diagnostic_vm_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class CcnMetricDb(Base):
    __tablename__ = "ccn_metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    measured_at: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True, nullable=False
    )
    item_hash: Mapped[str] = mapped_column(
        ForeignKey("messages.item_hash", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    node_id: Mapped[str] = mapped_column(String, nullable=False)
    base_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    base_latency_ipv4: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    metrics_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    aggregate_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    file_download_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pending_messages: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    eth_height_remaining: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
