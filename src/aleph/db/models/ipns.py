import datetime as dt
from typing import Optional

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy_utils import ChoiceType

from aleph.types.ipns import IpnsStatus

from .base import Base


class IpnsRecordDb(Base):
    """
    Current state of an IPNS registration.

    One row per (name, owner): the same name registered by two different
    addresses results in two rows, each billed independently. Only the
    holder of the Ed25519 key behind the name can produce records that
    verify, so this cannot be abused beyond paying to track a name.
    """

    __tablename__ = "ipns_records"

    # Canonical base36 IPNS name (k51..., 62 chars).
    name: Mapped[str] = mapped_column(String, primary_key=True)
    owner: Mapped[str] = mapped_column(String, primary_key=True)
    # Latest STORE message that changed this registration.
    item_hash: Mapped[str] = mapped_column(String, nullable=False)
    # Latest verified signed IPNS record (raw protobuf bytes).
    record: Mapped[Optional[bytes]] = mapped_column(LargeBinary, nullable=True)
    record_sequence: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    # Record end-of-life: republishing stops past this point.
    record_validity: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    # Paid size quota in MiB.
    max_size_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    # CID the name currently resolves to (pinned within quota).
    resolved_cid: Mapped[Optional[str]] = mapped_column(
        ForeignKey("files.hash"), nullable=True
    )
    last_resolved: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    last_republished: Mapped[Optional[dt.datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    status: Mapped[IpnsStatus] = mapped_column(
        ChoiceType(IpnsStatus), nullable=False, default=IpnsStatus.OK
    )
    created: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


Index("ix_ipns_records_owner", IpnsRecordDb.owner)
