import datetime as dt
from typing import List, Optional

from sqlalchemy import delete, select, update

from aleph.db.models.cron_jobs import CronJobDb
from aleph.types.db_session import AsyncDbSession


async def get_cron_jobs(session: AsyncDbSession) -> List[CronJobDb]:
    select_stmt = select(CronJobDb)

    return (await session.execute(select_stmt)).scalars().all()


async def get_cron_job(session: AsyncDbSession, id: str) -> Optional[CronJobDb]:
    select_stmt = select(CronJobDb).where(CronJobDb.id == id)

    return (await session.execute(select_stmt)).scalar_one_or_none()


async def update_cron_job(
    session: AsyncDbSession, id: str, last_run: dt.datetime
) -> None:
    update_stmt = update(CronJobDb).values(last_run=last_run).where(CronJobDb.id == id)

    await session.execute(update_stmt)


async def delete_cron_job(session: AsyncDbSession, id: str) -> None:
    delete_stmt = delete(CronJobDb).where(CronJobDb.id == id)

    await session.execute(delete_stmt)
