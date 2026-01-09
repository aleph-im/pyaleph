import datetime as dt
from typing import List, Optional, Sequence

from sqlalchemy import delete, select, update

from aleph.db.models.cron_jobs import CronJobDb
from aleph.types.db_session import DbSession


def get_cron_jobs(session: DbSession) -> Sequence[CronJobDb]:
    select_stmt = select(CronJobDb)

    return (session.execute(select_stmt)).scalars().all()


def get_cron_job(session: DbSession, id: str) -> Optional[CronJobDb]:
    select_stmt = select(CronJobDb).where(CronJobDb.id == id)

    return (session.execute(select_stmt)).scalar_one_or_none()


def update_cron_job(session: DbSession, id: str, last_run: dt.datetime) -> None:
    update_stmt = update(CronJobDb).values(last_run=last_run).where(CronJobDb.id == id)

    session.execute(update_stmt)


def delete_cron_job(session: DbSession, id: str) -> None:
    delete_stmt = delete(CronJobDb).where(CronJobDb.id == id)

    session.execute(delete_stmt)
