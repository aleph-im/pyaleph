import datetime as dt

from sqlalchemy import TIMESTAMP, Column, Integer, String

from .base import Base


class CronJobDb(Base):
    __tablename__ = "cron_jobs"

    id: str = Column(String, primary_key=True)
    interval: int = Column(Integer, nullable=False)
    last_run: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
