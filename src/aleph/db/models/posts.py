import datetime as dt
from typing import Any, Optional

from sqlalchemy import Column, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped

from aleph.types.channel import Channel
from .base import Base


class PostDb(Base):
    __tablename__ = "posts"

    item_hash: Mapped[str] = Column(String, primary_key=True)
    owner: Mapped[str] = Column(String, nullable=False, index=True)
    type: Mapped[Optional[str]] = Column(String, nullable=True, index=True)
    ref: Mapped[Optional[str]] = Column(String, nullable=True)
    amends: Mapped[Optional[str]] = Column(
        ForeignKey("posts.item_hash"), nullable=True, index=True
    )
    channel: Mapped[Optional[Channel]] = Column(String, nullable=True)
    content: Mapped[Any] = Column(JSONB, nullable=False)
    creation_datetime: Mapped[dt.datetime] = Column(TIMESTAMP(timezone=True), nullable=False)

    latest_amend: Mapped[Optional[str]] = Column(String, nullable=True)
