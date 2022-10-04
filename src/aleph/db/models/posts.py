import datetime as dt
from typing import Any, Optional

from sqlalchemy import Column, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB

from aleph.types.channel import Channel
from .base import Base


class PostDb(Base):
    __tablename__ = "posts"

    item_hash: str = Column(String, primary_key=True)
    owner: str = Column(String, nullable=False, index=True)
    type: Optional[str] = Column(String, nullable=True, index=True)
    ref: Optional[str] = Column(String, nullable=True)
    amends: Optional[str] = Column(
        ForeignKey("posts.item_hash"), nullable=True, index=True
    )
    channel: Optional[Channel] = Column(String, nullable=True)
    content: Any = Column(JSONB, nullable=False)
    creation_datetime: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)

    latest_amend: Optional[str] = Column(String, nullable=True)
