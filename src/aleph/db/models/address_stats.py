from typing import Optional

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from aleph.db.models.base import Base


class AddressStats(Base):
    __tablename__ = "address_stats_mat_view"
    __mapper_args__ = {"confirm_deleted_rows": False}

    address: Mapped[str] = mapped_column(String, primary_key=True)
    type: Mapped[Optional[str]] = mapped_column(String, primary_key=True)
    nb_messages: Mapped[int] = mapped_column(Integer, nullable=False)
