import datetime as dt

from aleph.jobs.cron.balance_job import BalanceCronJob
from aleph.jobs.cron.cron_job import CronJob
import pytest
import pytest_asyncio

from aleph.db.accessors.files import get_file 
from aleph.services.storage.engine import StorageEngine
from aleph.services.storage.garbage_collector import GarbageCollector
from aleph.storage import StorageService
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileType


@pytest.fixture
def cron_job(
    session_factory: DbSessionFactory
) -> CronJob:
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
    session_factory: DbSessionFactory,
    cron_job: CronJob,
    cron_run_datetime: dt.datetime,
):
    with session_factory() as session:
        await cron_job.run(now=cron_run_datetime)
        session.commit()
