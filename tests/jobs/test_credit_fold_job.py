import datetime as dt
from types import SimpleNamespace

import pytest
from configmanager import Config
from services.test_credit_inputs import PE, PS, attestation, pricing  # noqa: F401

from aleph.db.accessors.credit_ledger import get_fold_state
from aleph.db.models.credit_ledger import CreditPeriodQuoteDb
from aleph.db.models.cron_jobs import CronJobDb
from aleph.jobs.cron.credit_fold_job import CreditFoldCronJob
from aleph.services.credits.lifecycle import FoldConfig
from aleph.types.db_session import DbSessionFactory


def fold_config() -> FoldConfig:
    return FoldConfig(
        start_period=PS,
        genesis_period=None,
        genesis_hash=None,
        grace_seconds=10800,
        checkpoint_every_hours=24,
    )


def cron_row() -> CronJobDb:
    return CronJobDb(
        id="credit_fold",
        interval=300,
        last_run=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
    )


def fake_indexer(mocker):
    indexer = mocker.Mock()
    indexer.cfg = SimpleNamespace(aleph="0xALEPH")
    indexer.index = mocker.AsyncMock(return_value=0)
    indexer.requote_unquoted = mocker.AsyncMock(return_value=0)
    indexer.quote = mocker.AsyncMock(return_value=(5, 10**14))
    indexer.web3.eth.get_block = mocker.AsyncMock(side_effect=ConnectionError("down"))
    return indexer


def set_cursor(session_factory: DbSessionFactory, time: dt.datetime, block: int):
    with session_factory() as session:
        state = get_fold_state(session)
        state.chain_cursor_time = time
        state.chain_cursor_block = block
        session.commit()


@pytest.mark.asyncio
async def test_cron_job_chain_failure_does_not_block_fold(
    session_factory: DbSessionFactory,
    mock_config: Config,
    mocker,
    pricing,  # noqa: F811
):
    set_cursor(session_factory, PE, 1000)
    with session_factory() as session:
        session.add_all(attestation([]))
        session.commit()

    indexer = fake_indexer(mocker)
    indexer.index.side_effect = ConnectionError("rpc down")
    indexer.requote_unquoted.side_effect = ConnectionError("rpc down")
    job = CreditFoldCronJob(
        session_factory=session_factory,
        settings_getter=lambda: mock_config,
        indexer=indexer,
        cfg=fold_config(),
    )

    assert await job.run(PE + dt.timedelta(minutes=5), cron_row()) is True

    indexer.index.assert_awaited_once()
    indexer.requote_unquoted.assert_awaited_once()
    with session_factory() as session:
        state = get_fold_state(session)
        assert state.last_closed_period == PS
        # The cursor stays where it was.
        assert state.chain_cursor_block == 1000
        # The quote at the cursor block needed no extra RPC call.
        assert session.get(CreditPeriodQuoteDb, PS) is not None


@pytest.mark.asyncio
async def test_cron_job_without_indexer_still_folds(
    session_factory: DbSessionFactory, mock_config: Config, pricing  # noqa: F811
):
    set_cursor(session_factory, PE, 1000)
    with session_factory() as session:
        session.add_all(attestation([]))
        session.commit()

    job = CreditFoldCronJob(
        session_factory=session_factory,
        settings_getter=lambda: mock_config,
        indexer=None,
        cfg=fold_config(),
    )
    assert await job.run(PE, cron_row()) is True
    with session_factory() as session:
        assert get_fold_state(session).last_closed_period == PS
        assert session.get(CreditPeriodQuoteDb, PS) is None


@pytest.mark.asyncio
async def test_cron_job_commits_the_indexer_progress(
    session_factory: DbSessionFactory, mock_config: Config, mocker
):
    indexer = fake_indexer(mocker)

    async def index(session, state):
        state.chain_cursor_block = 1234
        state.chain_cursor_time = PS
        return 1

    indexer.index.side_effect = index
    job = CreditFoldCronJob(
        session_factory=session_factory,
        settings_getter=lambda: mock_config,
        indexer=indexer,
        cfg=fold_config(),
    )
    assert await job.run(PE, cron_row()) is True
    with session_factory() as session:
        state = get_fold_state(session)
        assert state.chain_cursor_block == 1234
        # Cursor below the period end: nothing closed.
        assert state.last_closed_period is None


def test_credit_fold_job_is_registered_only_with_a_start(
    session_factory: DbSessionFactory, mock_config: Config
):
    from aleph.commands import make_credit_fold_job

    credits = mock_config.aleph.credits
    assert make_credit_fold_job(mock_config, session_factory, None) is None

    credits.attestation.start_period.value = int(PS.timestamp())
    job = make_credit_fold_job(mock_config, session_factory, None)
    assert isinstance(job, CreditFoldCronJob)
    assert job.indexer is None  # chain.start_block unset
    assert job.cfg.first_period == PS

    credits.chain.start_block.value = 21_000_000
    job = make_credit_fold_job(mock_config, session_factory, None)
    assert job is not None and job.indexer is not None
    assert job.indexer.cfg.start_block == 21_000_000
