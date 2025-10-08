from decimal import Decimal

from aleph_message.models import PaymentType

from aleph.db.accessors.balances import get_credit_balance, get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.toolkit.constants import DAY
from aleph.types.db_session import DbSession
from aleph.types.message_status import (
    InsufficientBalanceException,
    InsufficientCreditException,
)


def validate_balance_for_payment(
    session: DbSession,
    address: str,
    message_cost: Decimal,
    payment_type: PaymentType,
) -> None:
    """
    Validates that an address has sufficient balance for a message cost based on payment type.

    For credit payments, validates minimum 1-day (24 hour) runtime requirement.
    For other payment types, validates standard balance requirements.

    Args:
        session: Database session
        address: Account address to check
        message_cost: Cost of the message (per seconds for credits)
        payment_type: Type of payment (hold, superfluid, credit)

    Raises:
        InsufficientBalanceException: When token balance is insufficient
        InsufficientCreditException: When credit balance is insufficient
    """
    if payment_type == PaymentType.credit:
        current_credit_balance = get_credit_balance(address=address, session=session)

        # Get current hourly credit cost for all running VMs
        current_credit_cost = get_total_cost_for_address(
            session=session, address=address, payment_type=payment_type
        )

        # Calculate total per-second cost (existing VMs + new VM)
        # Note: both current_credit_cost and message_cost are per-second rates
        total_per_second_cost = current_credit_cost + message_cost

        # Calculate minimum required credits for 1-day runtime
        required_credits = total_per_second_cost * DAY

        if current_credit_balance < required_credits:
            raise InsufficientCreditException(
                credit_balance=current_credit_balance,
                required_credits=required_credits,
                min_runtime_days=1,
            )
    else:
        # Handle regular balance checks for hold/superfluid payments
        current_balance = get_total_balance(address=address, session=session)
        current_cost = get_total_cost_for_address(
            session=session, address=address, payment_type=payment_type
        )

        required_balance = current_cost + message_cost

        if current_balance < required_balance:
            raise InsufficientBalanceException(
                balance=current_balance,
                required_balance=required_balance,
            )
