import abc
import asyncio
import datetime as dt
import logging
from typing import Coroutine, Dict, List

from configmanager import Config

from aleph.db.accessors.cron_jobs import get_cron_jobs, update_cron_job
from aleph.db.models.cron_jobs import CronJobDb
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory

LOGGER = logging.getLogger(__name__)


class BaseCronJob(abc.ABC):
    @abc.abstractmethod
    async def run(self, now: dt.datetime, job: CronJobDb) -> None:
        pass


class CronJob:
    def __init__(self, session_factory: DbSessionFactory, jobs: Dict[str, BaseCronJob]):
        self.session_factory = session_factory
        self.jobs = jobs

    async def __run_job(
        self,
        session: DbSession,
        cron_job: BaseCronJob,
        now: dt.datetime,
        job: CronJobDb,
    ):
        try:
            LOGGER.info(f"Starting '{job.id}' cron job check...")
            await cron_job.run(now, job)

            update_cron_job(session, job.id, now)

            LOGGER.info(f"'{job.id}' cron job ran successfully.")

        except Exception:
            LOGGER.exception(
                f"An unexpected error occurred during '{job.id}' cron job execution."
            )

    async def run(self, now: dt.datetime):
        with self.session_factory() as session:
            jobs = get_cron_jobs(session)
            jobs_to_run: List[Coroutine] = []

            for job in jobs:
                interval = dt.timedelta(seconds=job.interval)
                run_datetime = job.last_run + interval

                if now >= run_datetime:
                    cron_job = self.jobs.get(job.id)

                    if cron_job:
                        jobs_to_run.append(self.__run_job(session, cron_job, now, job))
                        LOGGER.info(
                            f"'{job.id}' cron job scheduled for running successfully."
                        )

            await asyncio.gather(*jobs_to_run)

            session.commit()


async def cron_job_task(config: Config, cron_job: CronJob) -> None:
    interval = dt.timedelta(hours=config.aleph.jobs.cron.period.value)

    # Start by waiting, this gives the node time to start up and process potential pending
    # messages that could pin files.
    LOGGER.info("Warming up cron job runner... next run: %s.", utc_now() + interval)
    await asyncio.sleep(interval.total_seconds())

    while True:
        try:
            now = utc_now()

            LOGGER.info("Starting cron job check...")
            await cron_job.run(now=now)
            LOGGER.info("Cron job ran successfully.")

            LOGGER.info("Next cron job run: %s.", now + interval)
            await asyncio.sleep(interval.total_seconds())

        except Exception:
            LOGGER.exception("An unexpected error occurred during cron job check.")
