"""Tests for the background credit-expiration task."""

import datetime as dt

import pytest
from sqlalchemy import select

from aleph.db.accessors.balances import _apply_grant_bucket
from aleph.db.models import AlephCreditBalanceDb
from aleph.services.credit_expiration import NOTIFY_CHANNEL, CreditExpirationTask
from aleph.toolkit.infinity import INFINITY
from aleph.types.db_session import DbSessionFactory


def _make_task(session_factory, mock_config):
    """The task only needs the config to open its LISTEN connection. The
    sync tests below exercise the helpers without opening that connection.
    """
    return CreditExpirationTask(session_factory=session_factory, config=mock_config)


@pytest.mark.asyncio
async def test_expiration_task_deletes_expired_buckets(
    session_factory: DbSessionFactory, mock_config
):
    """A sweep removes buckets whose ``expiration_date`` is in the past, and
    leaves both still-valid and sentinel buckets untouched.
    """
    past = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)
    future = dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        _apply_grant_bucket(session, "0xtask", 100, past)
        _apply_grant_bucket(session, "0xtask", 200, future)
        _apply_grant_bucket(session, "0xtask", 50, None)
        session.commit()

    _make_task(session_factory, mock_config)._delete_expired()

    with session_factory() as session:
        exps = sorted(
            row.expiration_date
            for row in session.execute(
                select(AlephCreditBalanceDb).where(
                    AlephCreditBalanceDb.address == "0xtask"
                )
            ).scalars()
        )
    assert past not in exps
    assert future in exps
    assert INFINITY in exps


@pytest.mark.asyncio
async def test_expiration_task_peek_ignores_sentinel(
    session_factory: DbSessionFactory, mock_config
):
    """The ``MIN(expiration_date)`` peek skips the sentinel so the task
    never schedules a wakeup for "never expires".
    """
    future = dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        _apply_grant_bucket(session, "0xpeek_sentinel", 100, None)
        session.commit()

    assert _make_task(session_factory, mock_config)._peek_next_expiration() is None

    with session_factory() as session:
        _apply_grant_bucket(session, "0xpeek_finite", 100, future)
        session.commit()

    assert _make_task(session_factory, mock_config)._peek_next_expiration() == future


@pytest.mark.asyncio
async def test_listen_callback_sets_event_on_notification(
    session_factory: DbSessionFactory, mock_config
):
    """The selector-side callback drains pending notifications and sets the
    task's event. The interaction between a Postgres NOTIFY and the
    ``add_reader`` plumbing is end-to-end exercised by integration tests
    rather than unit-tested here; this asserts the in-process glue.
    """

    class _FakeConn:
        def __init__(self) -> None:
            self.notifies = [object()]

        def poll(self) -> None:
            pass

    task = _make_task(session_factory, mock_config)
    task._listen_conn = _FakeConn()
    assert not task._event.is_set()
    task._on_listen_readable()
    assert task._event.is_set()
    # Subsequent poll with no notifications must not flap the event off.
    task._listen_conn.notifies = []
    task._event.clear()
    task._on_listen_readable()
    assert not task._event.is_set()


@pytest.mark.asyncio
async def test_writer_emits_notify_on_finite_expiration(
    session_factory: DbSessionFactory, mock_config
):
    """``_apply_grant_bucket`` issues ``NOTIFY credit_expiration_changed`` in
    the same transaction as the bucket insert, so commit delivers the wake.
    Sentinel buckets must not produce a NOTIFY.

    Captured via a LISTEN connection opened from the test config; this is
    the integration path the expiration task relies on.
    """
    import psycopg2.extensions

    from aleph.db.connection import make_db_url

    future = dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc)

    # Build the snoop DSN from the same config the session_factory uses.
    sqla_url = make_db_url(driver="psycopg2", config=mock_config)
    # Strip the SQLAlchemy dialect prefix so psycopg2 accepts the URL.
    prefix = "postgresql+psycopg2://"
    if sqla_url.startswith(prefix):
        snoop_dsn = "postgresql://" + sqla_url[len(prefix) :]
    else:
        snoop_dsn = sqla_url

    snoop = psycopg2.connect(snoop_dsn)
    snoop.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    snoop_cur = snoop.cursor()
    snoop_cur.execute(f"LISTEN {NOTIFY_CHANNEL};")
    try:
        # Finite-expiration grant: should emit NOTIFY on commit.
        with session_factory() as session:
            _apply_grant_bucket(session, "0xnotify_finite", 100, future)
            session.commit()
        snoop.poll()
        assert any(n.channel == NOTIFY_CHANNEL for n in snoop.notifies)
        snoop.notifies.clear()

        # Sentinel grant: must NOT emit NOTIFY.
        with session_factory() as session:
            _apply_grant_bucket(session, "0xnotify_sentinel", 100, None)
            session.commit()
        snoop.poll()
        assert not snoop.notifies
    finally:
        snoop_cur.close()
        snoop.close()
