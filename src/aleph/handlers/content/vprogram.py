import logging
from typing import List, Set

from aleph_message.models import PaymentType, VerifiableProgramContent

from aleph.db.models import MessageDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.services.cost import get_payment_type, get_total_and_detailed_costs
from aleph.services.cost_validation import validate_balance_for_payment
from aleph.types.db_session import DbSession
from aleph.types.message_status import InvalidMessageFormat, InvalidPaymentMethod

LOGGER = logging.getLogger(__name__)


def _get_vprogram_content(message: MessageDb) -> VerifiableProgramContent:
    content = message.parsed_content
    if not isinstance(content, VerifiableProgramContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for V-PROGRAM message: {message.item_hash}"
        )
    return content


class VProgramMessageHandler(ContentHandler):
    """Handles V-PROGRAM (verifiable program) messages.

    Phase 1 scope: validate the credit balance, persist costs, store and
    display the message. Measurement validation and CRN dispatch are later
    phases, so process/forget maintain no side tables: the message row is
    the source of truth.
    """

    async def check_balance(
        self, session: DbSession, message: MessageDb
    ) -> List[AccountCostsDb]:
        content = _get_vprogram_content(message)

        message_cost, costs = get_total_and_detailed_costs(
            session, content, message.item_hash
        )

        # The schema already restricts V-Programs to credit payment; keep
        # the pipeline error explicit in case the model ever loosens.
        payment_type = get_payment_type(content)
        if payment_type != PaymentType.credit:
            raise InvalidPaymentMethod()

        validate_balance_for_payment(
            session=session,
            address=content.address,
            message_cost=message_cost,
            payment_type=payment_type,
        )

        return costs

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        # No side tables in phase 1.
        pass

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        # No side tables and no update chain to clean up in phase 1. Costs
        # are removed by the generic forget path.
        return set()
