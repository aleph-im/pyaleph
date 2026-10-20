import datetime as dt
import logging
from typing import Any, Callable, Optional

from configmanager import Config

from aleph.db.accessors.credit_ledger import get_fold_state
from aleph.db.models.cron_jobs import CronJobDb
from aleph.jobs.cron.cron_job import BaseCronJob
from aleph.services.credits.lifecycle import (
    FoldConfig,
    close_ready_periods,
    load_genesis,
)
from aleph.services.credits.topups import TopupIndexer
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)


class CreditFoldCronJob(BaseCronJob):
    """
    Indexes the chain top-ups, re-quotes the unquoted ones, then folds every
    closable period. Each phase runs in its own session and its failure is
    logged without stopping the next one: a chain outage must not stop the
    folding of periods the cursor already covers.
    """

    def __init__(
        self,
        session_factory: DbSessionFactory,
        settings_getter: Callable[[], Config],
        indexer: Optional[TopupIndexer],
        cfg: FoldConfig,
        storage_service: Optional[Any] = None,
    ):
        self.session_factory = session_factory
        self.settings_getter = settings_getter
        self.indexer = indexer
        self.cfg = cfg
        # Retries a genesis load that failed at startup (repair_node).
        self.storage_service = storage_service

    async def _index(self) -> None:
        if self.indexer is None:
            LOGGER.warning(
                "aleph.credits.chain.start_block is not set: top-ups are not indexed"
            )
            return
        try:
            with self.session_factory() as session:
                state = get_fold_state(session)
                new_rows = await self.indexer.index(session, state)
                session.commit()
            LOGGER.info("Indexed %d new top-up event(s)", new_rows)
        except Exception:
            LOGGER.exception("Credit top-up indexing failed")
        try:
            with self.session_factory() as session:
                fixed = await self.indexer.requote_unquoted(session)
                session.commit()
            if fixed:
                LOGGER.info("Re-quoted %d top-up(s)", fixed)
        except Exception:
            LOGGER.exception("Re-quoting unquoted top-ups failed")

    async def _load_genesis(self) -> None:
        cfg = self.cfg
        if (
            self.storage_service is None
            or not cfg.genesis_hash
            or cfg.genesis_period is None
        ):
            return
        try:
            with self.session_factory() as session:
                if get_fold_state(session).genesis_loaded_hash == cfg.genesis_hash:
                    return
                await load_genesis(
                    session,
                    self.storage_service,
                    cfg.genesis_hash,
                    cfg.genesis_period,
                )
                session.commit()
        except Exception:
            LOGGER.exception("Loading credit genesis %s failed", cfg.genesis_hash)

    async def run(self, now: dt.datetime, job: CronJobDb) -> bool:
        await self._index()
        await self._load_genesis()
        try:
            closed = await close_ready_periods(
                self.session_factory,
                now,
                self.settings_getter(),
                self.cfg,
                indexer=self.indexer,
            )
            LOGGER.info("Credit fold closed %d period(s)", closed)
        except Exception:
            LOGGER.exception("Credit fold failed")
        return True
