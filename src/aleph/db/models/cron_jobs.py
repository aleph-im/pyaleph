import datetime as dt

from sqlalchemy import TIMESTAMP, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class CronJobDb(Base):
    __tablename__ = "cron_jobs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    # Interval is specified in seconds
    interval: Mapped[int] = mapped_column(Integer, nullable=False)
    last_run: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
