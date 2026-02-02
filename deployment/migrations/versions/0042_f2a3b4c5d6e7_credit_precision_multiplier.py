"""Credit precision multiplier - 10,000x

Revision ID: f2a3b4c5d6e7
Revises: d6539a42cd51
Create Date: 2026-01-29

Multiplies all credit values by 10,000 to support new precision:
1 USD = 1,000,000 credits (previously 100 credits).

Only entries with message_timestamp < CUTOFF_TIMESTAMP are multiplied.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "f2a3b4c5d6e7"
down_revision = "d6539a42cd51"
branch_labels = None
depends_on = None

MULTIPLIER = 10000
# Must match CREDIT_PRECISION_CUTOFF_TIMESTAMP in constants.py (1769990400)
# Format: 'YYYY-MM-DD HH:MM:SS+00' (UTC timezone)
CUTOFF_TIMESTAMP = "2026-02-02 00:00:00+00"


def upgrade() -> None:
    # 1. Update credit_history.amount only for entries BEFORE the cutoff
    op.execute(
        text(
            f"""
            UPDATE credit_history
            SET amount = amount * {MULTIPLIER}
            WHERE message_timestamp < '{CUTOFF_TIMESTAMP}'::timestamptz
        """
        )
    )

    # 2. Clear credit_balances cache - will be recalculated from history on next access
    #    This ensures balances are computed correctly from the updated history
    op.execute(text("TRUNCATE TABLE credit_balances"))

    # 3. Update account_costs.cost_credit for messages created BEFORE the cutoff
    #    Join with messages table to get the message creation time
    op.execute(
        text(
            f"""
            UPDATE account_costs
            SET cost_credit = cost_credit * {MULTIPLIER}
            FROM messages
            WHERE account_costs.item_hash = messages.item_hash
              AND messages.time < '{CUTOFF_TIMESTAMP}'::timestamptz
        """
        )
    )


def downgrade() -> None:
    # Reverse: divide by multiplier (only for entries before cutoff)
    op.execute(
        text(
            f"""
            UPDATE credit_history
            SET amount = amount / {MULTIPLIER}
            WHERE message_timestamp < '{CUTOFF_TIMESTAMP}'::timestamptz
        """
        )
    )

    op.execute(text("TRUNCATE TABLE credit_balances"))

    op.execute(
        text(
            f"""
            UPDATE account_costs
            SET cost_credit = cost_credit / {MULTIPLIER}
            FROM messages
            WHERE account_costs.item_hash = messages.item_hash
              AND messages.time < '{CUTOFF_TIMESTAMP}'::timestamptz
        """
        )
    )
