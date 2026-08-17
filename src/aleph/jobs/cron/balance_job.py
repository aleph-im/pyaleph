import asyncio
import datetime as dt
import logging
from typing import List, cast

from aleph_message.models import ItemHash, MessageType, PaymentType
from sqlalchemy import update
from sqlalchemy.engine import CursorResult

from aleph.db.accessors.balances import get_total_balance, get_updated_balance_accounts
from aleph.db.accessors.cost import get_total_costs_for_address_grouped_by_message
from aleph.db.accessors.files import update_file_pin_grace_period
from aleph.db.accessors.messages import (
    delete_removed_message,
    get_message_by_item_hash,
    get_message_status,
    make_message_status_upsert_query,
    upsert_removed_message_size,
)
from aleph.db.models.cron_jobs import CronJobDb
from aleph.db.models.messages import MessageDb, MessageStatusDb
from aleph.jobs.cron.cron_job import BaseCronJob
from aleph.services.cost import calculate_storage_size
from aleph.toolkit.constants import STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT, MiB
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import MessageStatus

LOGGER = logging.getLogger(__name__)

# Commit and yield the event loop every this many messages, so a large account
# cannot hold one long transaction (and the message_counts counter-row locks its
# trigger takes) or monopolise the shared event loop.
COMMIT_CHUNK = 500

# Cap the mutations performed per run so a burst (many accounts, or one account
# with tens of thousands of resources) drains over several bounded ticks instead
# of one runaway tick. The remainder is idempotently handled by the next run.
MAX_MESSAGES_PER_RUN = 5000


class BalanceCronJob(BaseCronJob):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        max_unauthenticated_upload_file_size: int,
    ):
        self.session_factory = session_factory
        self.max_unauthenticated_upload_file_size = max_unauthenticated_upload_file_size

    async def run(self, now: dt.datetime, job: CronJobDb):
        with self.session_factory() as session:
            accounts = get_updated_balance_accounts(session, job.last_run)

            LOGGER.info(f"Checking '{len(accounts)}' updated account balances...")

            # Budget the mutations performed this run so one tick cannot process
            # an unbounded batch; the remainder is idempotently handled next run.
            budget = MAX_MESSAGES_PER_RUN

            for address in accounts:
                if budget <= 0:
                    LOGGER.info(
                        "Per-run message cap (%d) reached; remaining accounts "
                        "will be handled on the next run.",
                        MAX_MESSAGES_PER_RUN,
                    )
                    break

                remaining_balance = get_total_balance(session, address)

                to_delete = []
                to_recover = []

                hold_costs = get_total_costs_for_address_grouped_by_message(
                    session, address, PaymentType.hold
                )

                for index, (item_hash, height, cost, _) in enumerate(
                    hold_costs, start=1
                ):
                    LOGGER.debug(
                        "Checking %s message, with height %s and cost %s",
                        item_hash,
                        height,
                        cost,
                    )

                    should_remove = remaining_balance < cost and (
                        height is not None
                        and height >= STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT
                    )
                    remaining_balance = max(0, remaining_balance - cost)

                    status = get_message_status(session, item_hash)
                    if status is None:
                        continue

                    if should_remove:
                        if (
                            status.status != MessageStatus.REMOVING
                            and status.status != MessageStatus.REMOVED
                        ):
                            to_delete.append(item_hash)
                    elif status.status == MessageStatus.REMOVING:
                        to_recover.append(item_hash)

                    # Yield during the read-only scan so a large account cannot
                    # monopolise the shared event loop.
                    if index % COMMIT_CHUNK == 0:
                        await asyncio.sleep(0)

                if to_delete:
                    LOGGER.info(
                        f"'{len(to_delete)}' messages to delete for account '{address}'..."
                    )
                    budget -= await self.delete_messages(session, to_delete[:budget])

                if budget > 0 and to_recover:
                    LOGGER.info(
                        f"'{len(to_recover)}' messages to recover for account '{address}'..."
                    )
                    budget -= await self.recover_messages(session, to_recover[:budget])

                session.commit()

    async def delete_messages(
        self, session: DbSession, messages: List[ItemHash]
    ) -> int:
        for index, item_hash in enumerate(messages, start=1):
            message = get_message_by_item_hash(session, item_hash)

            if message is None:
                continue

            if message.type == MessageType.store:
                storage_size_mib = calculate_storage_size(
                    session, message.parsed_content
                )

                if storage_size_mib and storage_size_mib <= (
                    self.max_unauthenticated_upload_file_size / MiB
                ):
                    continue

            now = utc_now()
            delete_by = now + dt.timedelta(hours=24 + 1)

            if message.type == MessageType.store:
                update_file_pin_grace_period(
                    session=session,
                    item_hash=item_hash,
                    delete_by=delete_by,
                )

            result = cast(
                CursorResult,
                session.execute(
                    make_message_status_upsert_query(
                        item_hash=item_hash,
                        new_status=MessageStatus.REMOVING,
                        reception_time=now,
                        where=(MessageStatusDb.status == MessageStatus.PROCESSED),
                    )
                ),
            )

            # Only when this call actually performed the PROCESSED->REMOVING
            # flip: a message on another transition (e.g. already REMOVED)
            # must not get its denormalized status clobbered or a removal
            # record written under it.
            if result.rowcount > 0:
                # Dual-write to messages table (trigger handles message_counts)
                session.execute(
                    update(MessageDb)
                    .where(MessageDb.item_hash == item_hash)
                    .values(status_value=MessageStatus.REMOVING)
                )
                # Snapshot the file size while the files row still exists;
                # the garbage collector stamps removed_at at
                # REMOVING->REMOVED.
                upsert_removed_message_size(session=session, item_hash=item_hash)

            # Commit in chunks so the transaction — and the message_counts
            # counter-row locks its trigger takes — stays small, and yield so
            # other event-loop tasks (API, other crons) keep running.
            if index % COMMIT_CHUNK == 0:
                session.commit()
                await asyncio.sleep(0)

        return len(messages)

    async def recover_messages(
        self, session: DbSession, messages: List[ItemHash]
    ) -> int:
        for index, item_hash in enumerate(messages, start=1):
            message = get_message_by_item_hash(session, item_hash)
            if message is None:
                continue

            if message.type == MessageType.store:
                update_file_pin_grace_period(
                    session=session,
                    item_hash=item_hash,
                    delete_by=None,
                )

            result = cast(
                CursorResult,
                session.execute(
                    make_message_status_upsert_query(
                        item_hash=item_hash,
                        new_status=MessageStatus.PROCESSED,
                        reception_time=utc_now(),
                        where=(MessageStatusDb.status == MessageStatus.REMOVING),
                    )
                ),
            )

            # Only when this call actually performed the REMOVING->PROCESSED
            # flip: if the garbage collector already finalized
            # REMOVING->REMOVED, the message must not reappear as processed
            # and the removal record (size/removed_at) must survive.
            if result.rowcount > 0:
                # Dual-write to messages table (trigger handles message_counts)
                session.execute(
                    update(MessageDb)
                    .where(MessageDb.item_hash == item_hash)
                    .values(status_value=MessageStatus.PROCESSED)
                )
                delete_removed_message(session=session, item_hash=item_hash)

            # Commit in chunks so the transaction — and the message_counts
            # counter-row locks its trigger takes — stays small, and yield so
            # other event-loop tasks (API, other crons) keep running.
            if index % COMMIT_CHUNK == 0:
                session.commit()
                await asyncio.sleep(0)

        return len(messages)
