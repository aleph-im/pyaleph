from sqlalchemy import BigInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class MessageCountsDb(Base):
    __tablename__ = "message_counts"

    type: Mapped[str] = mapped_column(String, primary_key=True, default="")
    status: Mapped[str] = mapped_column(String, primary_key=True, default="")
    sender: Mapped[str] = mapped_column(String, primary_key=True, default="")
    owner: Mapped[str] = mapped_column(String, primary_key=True, default="")
    channel: Mapped[str] = mapped_column(String, primary_key=True, default="")
    payment_type: Mapped[str] = mapped_column(String, primary_key=True, default="")
    row_count: Mapped[int] = mapped_column(
        "count", BigInteger, nullable=False, default=0
    )
