import datetime as dt
from typing import Any, Optional

from sqlalchemy import TIMESTAMP, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aleph.types.channel import Channel

from .base import Base


class PostDb(Base):
    __tablename__ = "posts"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    owner: Mapped[str] = mapped_column(String, nullable=False, index=True)
    type: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
    ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    amends: Mapped[Optional[str]] = mapped_column(
        ForeignKey("posts.item_hash"), nullable=True, index=True
    )
    channel: Mapped[Optional[Channel]] = mapped_column(String, nullable=True)
    content: Mapped[Any] = mapped_column(JSONB, nullable=False)
    creation_datetime: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    latest_amend: Mapped[Optional[str]] = mapped_column(String, nullable=True)
