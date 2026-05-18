from typing import Optional

from sqlalchemy import BigInteger, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class CrnMetricDb(Base):
    __tablename__ = "crn_metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    item_hash: Mapped[str] = mapped_column(
        ForeignKey("messages.item_hash", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    node_id: Mapped[str] = mapped_column(String, nullable=False)
    measured_at: Mapped[float] = mapped_column(Float, nullable=False)
    base_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    base_latency_ipv4: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    full_check_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    diagnostic_vm_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class CcnMetricDb(Base):
    __tablename__ = "ccn_metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    item_hash: Mapped[str] = mapped_column(
        ForeignKey("messages.item_hash", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    node_id: Mapped[str] = mapped_column(String, nullable=False)
    measured_at: Mapped[float] = mapped_column(Float, nullable=False)
    base_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    base_latency_ipv4: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    metrics_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    aggregate_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    file_download_latency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pending_messages: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    eth_height_remaining: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
