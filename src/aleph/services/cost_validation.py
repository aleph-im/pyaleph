from decimal import Decimal

from aleph_message.models import PaymentType

from aleph.db.accessors.balances import get_credit_balance, get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
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
        message_cost: Cost of the message (per hour for credits)
        payment_type: Type of payment (hold, superfluid, credit)

    Raises:
        InsufficientBalanceException: When token balance is insufficient
        InsufficientCreditException: When credit balance is insufficient
    """
    if payment_type == PaymentType.credit:
        current_credit_balance = get_credit_balance(address=address, session=session)
        current_credit_cost = get_total_cost_for_address(
            session=session, address=address, payment_type=PaymentType.credit
        )

        # Calculate minimum required credits for 1-day runtime (24 hours)
        daily_credit_cost = message_cost * 24  # Assuming message_cost is per hour
        required_credits = current_credit_cost + daily_credit_cost

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
