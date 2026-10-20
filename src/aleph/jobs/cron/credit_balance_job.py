import asyncio
import datetime as dt
import logging
from typing import List, cast

from aleph_message.models import ItemHash, MessageType, PaymentType
from sqlalchemy import update
from sqlalchemy.engine import CursorResult

from aleph.config import get_config
from aleph.db.accessors.balances import get_updated_credit_balance_accounts
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
from aleph.services.credits.balance import (
    addresses_with_ledger_activity_since,
    fold_lag_exceeded,
    get_credit_balance_any,
    owners_of_live_credit_resources,
    serve_v2_active,
)
from aleph.toolkit.constants import CREDIT_ONLY_CUTOFF_TIMESTAMP, DAY, MiB
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

# Serving the v2 ledger, the accounts to check are those with ledger rows in
# recent periods. A period's rows are written when the fold closes it, up to
# the attestation grace (3 h by default) plus the top-up finality and the cron
# cadence after its end, so the scan reaches back this far before the last
# run. Over-inclusion is harmless: the check is idempotent. Limit: a period
# closed more than 6 h after the last run (a fold stalled longer than that)
# writes rows this scan no longer reaches; only accounts with live credit
# resources (always checked) are then re-examined.
V2_ACTIVITY_LOOKBACK = dt.timedelta(hours=6)

# Beyond the attestation grace, the fold should close a period within this.
FOLD_LAG_MARGIN = dt.timedelta(hours=2)


class CreditBalanceCronJob(BaseCronJob):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        max_unauthenticated_upload_file_size: int,
    ):
        self.session_factory = session_factory
        self.max_unauthenticated_upload_file_size = max_unauthenticated_upload_file_size

    async def run(self, now: dt.datetime, job: CronJobDb) -> bool:
        with self.session_factory() as session:
            serve_v2 = serve_v2_active(session)
            if serve_v2:
                self.check_fold_lag(session, now)
                # Accounts with recent ledger rows (top-ups, transfers, billed
                # hours) and accounts with live credit-paid resources, which
                # keep accruing even when the fold writes nothing.
                accounts = sorted(
                    set(
                        addresses_with_ledger_activity_since(
                            session, job.last_run - V2_ACTIVITY_LOOKBACK
                        )
                    )
                    | set(owners_of_live_credit_resources(session))
                )
            else:
                accounts = get_updated_credit_balance_accounts(session, job.last_run)

            LOGGER.info(
                f"Checking '{len(accounts)}' updated credit balance accounts..."
            )

            # Budget the mutations performed this run so one tick cannot process
            # an unbounded batch. `deferred` tracks whether any work was left
            # undone; when True we return False so the caller does not advance
            # last_run and the next tick re-queries the same accounts to drain
            # the remainder (the account query is delta-based on last_run).
            budget = MAX_MESSAGES_PER_RUN
            deferred = False
            checked = 0

            for address in accounts:
                if budget <= 0:
                    deferred = True
                    LOGGER.info(
                        "Per-run message cap (%d) reached; deferring remaining "
                        "accounts to the next run.",
                        MAX_MESSAGES_PER_RUN,
                    )
                    break

                remaining_credits = get_credit_balance_any(session, address)
                checked += 1

                to_delete = []
                to_recover = []

                # Ledger addresses are lowercased (EVM): match the costs of
                # the account whatever case its messages used.
                credit_costs = get_total_costs_for_address_grouped_by_message(
                    session, address, PaymentType.credit, case_insensitive=serve_v2
                )

                for index, (item_hash, _height, cost, _) in enumerate(
                    credit_costs, start=1
                ):
                    # For credits, check if balance is insufficient for minimum 1-day
                    # runtime. Costs in account_costs are stored per-second, so multiply
                    # by DAY (seconds per day) to get the per-day requirement, matching
                    # the ingest-time check in cost_validation.validate_balance_for_payment.
                    daily_cost = cost * DAY
                    should_remove = remaining_credits < daily_cost
                    # Reserve a full day of this resource's runtime so the next
                    # resource is checked against what is actually left, matching the
                    # per-day threshold above (decrementing by the per-second cost
                    # would let an account keep many resources it cannot afford for a
                    # day, which the ingest-time check already rejects). Decrement
                    # before the status lookup/None-check so a cost row without a
                    # message-status row still reserves balance, preserving the
                    # original running-balance semantics.
                    remaining_credits = max(0, remaining_credits - daily_cost)

                    status = get_message_status(session, item_hash)
                    if status is None:
                        continue

                    LOGGER.debug(
                        "Checking credit message %s with cost %s credits",
                        item_hash,
                        cost,
                    )

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
                    processing = min(len(to_delete), budget)
                    if len(to_delete) > budget:
                        deferred = True
                    LOGGER.info(
                        f"Deleting '{processing}' of '{len(to_delete)}' credit-paid "
                        f"messages for account '{address}'..."
                    )
                    budget -= await self.delete_messages(session, to_delete[:budget])

                if to_recover:
                    # budget may be 0 here if deletes consumed it this account.
                    if budget <= 0 or len(to_recover) > budget:
                        deferred = True
                    if budget > 0:
                        processing = min(len(to_recover), budget)
                        LOGGER.info(
                            f"Recovering '{processing}' of '{len(to_recover)}' "
                            f"credit-paid messages for account '{address}'..."
                        )
                        budget -= await self.recover_messages(
                            session, to_recover[:budget]
                        )

                session.commit()

            LOGGER.info(
                "Checked %d of %d credit balance accounts (%s)",
                checked,
                len(accounts),
                "v2 ledger" if serve_v2 else "legacy",
            )
            return not deferred

    @staticmethod
    def check_fold_lag(session: DbSession, now: dt.datetime) -> None:
        """Log an ERROR (once per run) when the credit fold is stalled."""
        grace = dt.timedelta(
            seconds=int(get_config().aleph.credits.attestation.grace_seconds.value)
        )
        last_closed = fold_lag_exceeded(session, now, grace + FOLD_LAG_MARGIN)
        if last_closed is not None:
            LOGGER.error(
                "Credit fold is stalled: last closed period %s is more than %s "
                "behind; balances include a growing provisional accrual and "
                "removals rely on it",
                last_closed.isoformat(),
                grace + FOLD_LAG_MARGIN,
            )

    async def delete_messages(
        self, session: DbSession, messages: List[ItemHash]
    ) -> int:
        """Return the number of messages attempted, used by the caller to
        decrement the per-run budget. Skipped no-ops (missing message,
        small-file exception, or a status guard that prevents the flip) count
        on purpose: the budget bounds the number of delete/recover attempts —
        each a DB round-trip — not only the flips that succeed, because
        bounding per-tick work is what the cap exists to do."""
        for index, item_hash in enumerate(messages, start=1):
            message = get_message_by_item_hash(session, item_hash)

            if message is None:
                continue

            if message.type == MessageType.store:
                storage_size_mib = calculate_storage_size(
                    session, message.parsed_content
                )

                # Small file exception only applies to messages before the
                # credit-only cutoff. Use the observed (confirmation or
                # reception) time, never the sender-supplied time, so a backdated
                # STORE cannot claim the free legacy small-file exception.
                message_timestamp = message.observed_time.timestamp()
                is_legacy_message = message_timestamp < CREDIT_ONLY_CUTOFF_TIMESTAMP

                if (
                    is_legacy_message
                    and storage_size_mib
                    and storage_size_mib
                    <= (self.max_unauthenticated_upload_file_size / MiB)
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
        """Return the number of messages attempted, used by the caller to
        decrement the per-run budget. Skipped no-ops (missing message,
        small-file exception, or a status guard that prevents the flip) count
        on purpose: the budget bounds the number of delete/recover attempts —
        each a DB round-trip — not only the flips that succeed, because
        bounding per-tick work is what the cap exists to do."""
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
