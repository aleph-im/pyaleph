import datetime as dt
import logging
from typing import List

from aleph_message.models import MessageType, PaymentType

from aleph.db.accessors.balances import get_updated_balances
from aleph.db.accessors.cost import get_total_costs_for_address_grouped_by_message
from aleph.db.accessors.files import upsert_grace_period_file_pin
from aleph.db.accessors.messages import (
    get_message_by_item_hash,
    make_message_status_upsert_query,
)
from aleph.db.models.cron_jobs import CronJobDb
from aleph.db.models.messages import MessageStatusDb
from aleph.jobs.cron.cron_job import BaseCronJob
from aleph.toolkit.constants import STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import MessageStatus

LOGGER = logging.getLogger(__name__)


class BalanceCronJob(BaseCronJob):
    def __init__(self, session_factory: DbSessionFactory):
        self.session_factory = session_factory

    async def run(self, now: dt.datetime, job: CronJobDb):
        with self.session_factory() as session:
            balances = get_updated_balances(session, job.last_run)

            LOGGER.info(f"Checking '{len(balances)}' updated balances...")

            for address, balance in balances:
                to_delete = []
                to_recover = []

                hold_costs = get_total_costs_for_address_grouped_by_message(
                    session, address, PaymentType.hold
                )
                remaining_balance = balance

                for item_hash, height, cost, _ in hold_costs:
                    LOGGER.info(
                        f"Checking {item_hash} message, with height {height} and cost {cost}"
                    )

                    if remaining_balance < cost:
                        if (
                            height is not None
                            and height >= STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT
                        ):
                            # Check if it is STORE message and the size is greater than 25 MiB
                            to_delete.append(item_hash)
                    else:
                        to_recover.append(item_hash)

                    remaining_balance = max(0, remaining_balance - cost)

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

            if message is not None:
                now = utc_now()
                delete_by = now + dt.timedelta(hours=24 + 1)

                if message.type == MessageType.store:
                    upsert_grace_period_file_pin(
                        session=session,
                        file_hash=message.parsed_content.item_hash,
                        created=now,
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

            if message is not None:
                now = utc_now()
                delete_by = None

                if message.type == MessageType.store:
                    upsert_grace_period_file_pin(
                        session=session,
                        file_hash=message.parsed_content.item_hash,
                        created=now,
                        delete_by=delete_by,
                    )

                session.execute(
                    make_message_status_upsert_query(
                        item_hash=item_hash,
                        new_status=MessageStatus.PROCESSED,
                        reception_time=utc_now(),
                        where=(MessageStatusDb.status == MessageStatus.REMOVING),
                    )
                )
