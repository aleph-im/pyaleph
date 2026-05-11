"""Background task that drops expired credit_balances bucket rows.

Replaces the read-path FIFO walk that previously filtered expired credits
on every balance read. The task sleeps until the next expiration timestamp
present in ``credit_balances`` and is woken via a Postgres ``LISTEN`` on
``credit_expiration_changed``. Writers in any worker process ``NOTIFY``
that channel inside their transaction; Postgres delivers the notification
after commit so the wake reflects committed state.

Single instance per CCN process (the main process — message processing
runs in spawned subprocesses, so the in-process ``asyncio.Event`` is *not*
shared across them; the database is the rendezvous point).
"""

import asyncio
import logging
from typing import Optional

import psycopg2
import psycopg2.extensions
from configmanager import Config
from sqlalchemy import delete, func, select

from aleph.db.connection import make_db_url
from aleph.db.models import AlephCreditBalanceDb
from aleph.toolkit.infinity import INFINITY
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)

NOTIFY_CHANNEL = "credit_expiration_changed"

# Fallback timeout when no expiration is pending. Long enough to be cheap,
# short enough that a missed NOTIFY (e.g. transient driver glitch) recovers
# in bounded time. The NOTIFY path is the primary wake mechanism.
_IDLE_TIMEOUT_SECONDS = 60 * 60  # 1 hour


class CreditExpirationTask:
    """Single long-running task that deletes expired bucket rows.

    Holds a dedicated psycopg2 connection in autocommit mode for the LISTEN
    side (separate from the SQLAlchemy pool so the LISTEN doesn't tie up a
    regular connection). The connection's fd is registered with the event
    loop via ``loop.add_reader``: when Postgres pushes a notification, the
    reader callback drains ``connection.notifies`` and sets the local event.

    Each iteration:
      1. ``DELETE FROM credit_balances WHERE expiration_date <= now()``.
      2. ``SELECT MIN(expiration_date)`` excluding the ``infinity`` sentinel.
      3. ``asyncio.wait_for(event.wait(), timeout=delta)`` if there's a
         finite expiration, else unbounded ``event.wait()`` until a NOTIFY.
    """

    def __init__(self, session_factory: DbSessionFactory, config: Config) -> None:
        self.session_factory = session_factory
        self._config = config
        self._event = asyncio.Event()
        self._listen_conn: Optional[psycopg2.extensions.connection] = None
        self._fd: Optional[int] = None

    async def run(self) -> None:
        loop = asyncio.get_event_loop()
        self._listen_conn = self._open_listen_connection()
        self._fd = self._listen_conn.fileno()
        loop.add_reader(self._fd, self._on_listen_readable)
        LOGGER.info("Credit expiration task started (LISTEN %s)", NOTIFY_CHANNEL)
        try:
            while True:
                await self._tick()
        except asyncio.CancelledError:
            LOGGER.info("Credit expiration task cancelled")
            raise
        finally:
            if self._fd is not None:
                loop.remove_reader(self._fd)
            if self._listen_conn is not None:
                self._listen_conn.close()

    def _open_listen_connection(self) -> psycopg2.extensions.connection:
        dsn = make_db_url(
            driver="psycopg2",
            config=self._config,
            application_name="credit-expiration-listen",
        )
        # SQLAlchemy URL is psycopg2-compatible after stripping the dialect prefix.
        prefix = "postgresql+psycopg2://"
        if dsn.startswith(prefix):
            dsn = "postgresql://" + dsn[len(prefix) :]
        conn = psycopg2.connect(dsn)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(f"LISTEN {NOTIFY_CHANNEL};")
        cur.close()
        return conn

    def _on_listen_readable(self) -> None:
        """Selector callback: drain pending notifications and wake the task."""
        conn = self._listen_conn
        if conn is None:
            return
        try:
            conn.poll()
        except Exception:
            LOGGER.exception("LISTEN connection poll failed")
            return
        if conn.notifies:
            conn.notifies.clear()
            self._event.set()

    async def _tick(self) -> None:
        self._delete_expired()

        next_exp = self._peek_next_expiration()
        if next_exp is None:
            # Nothing pending: wait until a writer NOTIFY-s or the idle
            # timeout fires (defence-in-depth re-poll).
            try:
                await asyncio.wait_for(
                    self._event.wait(), timeout=_IDLE_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                pass
            finally:
                self._event.clear()
            return

        timeout = (next_exp - utc_now()).total_seconds()
        if timeout <= 0:
            # Already past the next expiration: re-sweep immediately.
            return

        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            self._event.clear()

    def _peek_next_expiration(self):
        with self.session_factory() as session:
            return session.execute(
                select(func.min(AlephCreditBalanceDb.expiration_date)).where(
                    AlephCreditBalanceDb.expiration_date > utc_now(),
                    AlephCreditBalanceDb.expiration_date < INFINITY,
                    AlephCreditBalanceDb.amount > 0,
                )
            ).scalar()

    def _delete_expired(self) -> None:
        with self.session_factory() as session:
            result = session.execute(
                delete(AlephCreditBalanceDb).where(
                    AlephCreditBalanceDb.expiration_date <= utc_now()
                )
            )
            session.commit()
            # session.execute(delete(...)) returns a CursorResult at runtime;
            # the static Result alias hides ``rowcount`` so cast for mypy.
            rowcount = getattr(result, "rowcount", 0)
            if rowcount:
                LOGGER.info("Dropped %d expired credit bucket(s)", rowcount)
