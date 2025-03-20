import datetime as dt
from decimal import Decimal
import logging

from aleph.db.accessors.balances import get_updated_balances
from aleph.db.accessors.cost import get_total_costs_for_address_grouped_by_message
from aleph.db.models.cron_jobs import CronJobDb
from aleph.jobs.cron.cron_job import BaseCronJob
from aleph.types.db_session import DbSessionFactory
from aleph_message.models import PaymentType

LOGGER = logging.getLogger(__name__)


class BalanceCronJob(BaseCronJob):
    def __init__(self, session_factory: DbSessionFactory):
        self.session_factory = session_factory

    async def run(self, now: dt.datetime, job: CronJobDb):
        with self.session_factory() as session:
            balances = get_updated_balances(session, job.last_run)

            LOGGER.info(f"Checking '{len(balances)}' updated balances...")

            for address, balance in balances:
                to_forget = []

                hold_costs = get_total_costs_for_address_grouped_by_message(session, address, PaymentType.hold)
                remaining_balance = Decimal(1) # balance

                for item_hash, cost, _ in hold_costs:
                    if remaining_balance <= 0:
                        break
                    elif remaining_balance >= cost:
                        remaining_balance -= cost
                    else:
                        to_forget.append(item_hash)

                if len(to_forget) > 0:
                    LOGGER.info(f"'{len(to_forget)}' messages to forget for account '{address}'...")
