import datetime as dt
import logging
from typing import List

from aleph_message.models import MessageType, PaymentType

from aleph.db.accessors.balances import get_total_balance, get_updated_balance_accounts
from aleph.db.accessors.cost import get_total_costs_for_address_grouped_by_message
from aleph.db.accessors.files import update_file_pin_grace_period
from aleph.db.accessors.messages import (
    get_message_by_item_hash,
    get_message_status,
    make_message_status_upsert_query,
)
from aleph.db.models.cron_jobs import CronJobDb
from aleph.db.models.messages import MessageStatusDb
from aleph.jobs.cron.cron_job import BaseCronJob
from aleph.services.cost import calculate_storage_size
from aleph.toolkit.constants import (
    MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT,
    MiB,
)
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import MessageStatus

LOGGER = logging.getLogger(__name__)


class BalanceCronJob(BaseCronJob):
    def __init__(self, session_factory: DbSessionFactory):
        self.session_factory = session_factory

    async def run(self, now: dt.datetime, job: CronJobDb):
        with self.session_factory() as session:
            accounts = get_updated_balance_accounts(session, job.last_run)

            LOGGER.info(f"Checking '{len(accounts)}' updated account balances...")

            for address in accounts:
                remaining_balance = get_total_balance(session, address)

                to_delete = []
                to_recover = []

                hold_costs = get_total_costs_for_address_grouped_by_message(
                    session, address, PaymentType.hold
                )

                for item_hash, height, cost, _ in hold_costs:
                    status = get_message_status(session, item_hash)

                    LOGGER.info(
                        f"Checking {item_hash} message, with height {height} and cost {cost}"
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
                    else:
                        if status.status == MessageStatus.REMOVING:
                            to_recover.append(item_hash)

                if len(to_delete) > 0:
                    LOGGER.info(
                        f"'{len(to_delete)}' messages to delete for account '{address}'..."
                    )
                    await self.delete_messages(session, to_delete)

                if len(to_recover) > 0:
                    LOGGER.info(
                        f"'{len(to_recover)}' messages to recover for account '{address}'..."
                    )
                    await self.recover_messages(session, to_recover)

                session.commit()

    async def delete_messages(self, session: DbSession, messages: List[str]):
        for item_hash in messages:
            message = get_message_by_item_hash(session, item_hash)

            if message is None:
                continue

            if message.type == MessageType.store:
                storage_size_mib = calculate_storage_size(
                    session, message.parsed_content
                )

                if storage_size_mib and storage_size_mib <= (
                    MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE / MiB
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

            session.execute(
                make_message_status_upsert_query(
                    item_hash=item_hash,
                    new_status=MessageStatus.REMOVING,
                    reception_time=now,
                    where=(MessageStatusDb.status == MessageStatus.PROCESSED),
                )
            )

    async def recover_messages(self, session: DbSession, messages: List[str]):
        for item_hash in messages:
            message = get_message_by_item_hash(session, item_hash)
            if message is None:
                continue

            if message.type == MessageType.store:
                update_file_pin_grace_period(
                    session=session,
                    item_hash=item_hash,
                    delete_by=None,
                )

            session.execute(
                make_message_status_upsert_query(
                    item_hash=item_hash,
                    new_status=MessageStatus.PROCESSED,
                    reception_time=utc_now(),
                    where=(MessageStatusDb.status == MessageStatus.REMOVING),
                )
            )
