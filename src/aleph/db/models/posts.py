import datetime as dt
from typing import Any, List, Optional

from sqlalchemy import TIMESTAMP, ForeignKey, String
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
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
    tags: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String), nullable=True)

    def __init__(self, **kwargs: Any) -> None:
        if "tags" not in kwargs:
            content = kwargs.get("content")
            if isinstance(content, dict):
                raw = content.get("tags")
                if isinstance(raw, list) and raw:
                    cleaned = [t for t in raw if isinstance(t, str)]
                    kwargs["tags"] = cleaned or None
        super().__init__(**kwargs)
