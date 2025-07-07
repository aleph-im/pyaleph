import datetime as dt

import pytest

from aleph.jobs.cron.balance_job import BalanceCronJob
from aleph.jobs.cron.cron_job import CronJob
from aleph.types.db_session import AsyncDbSessionFactory


@pytest.fixture
def cron_job(session_factory: AsyncDbSessionFactory) -> CronJob:
    return CronJob(
        session_factory=session_factory,
        jobs={"balance": BalanceCronJob(session_factory=session_factory)},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cron_run_datetime",
    [
        dt.datetime(2040, 1, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
    ],
)
async def test_balance_job_run(
    session_factory: AsyncDbSessionFactory,
    cron_job: CronJob,
    cron_run_datetime: dt.datetime,
):
    async with session_factory() as session:
        await cron_job.run(now=cron_run_datetime)
        await session.commit()
