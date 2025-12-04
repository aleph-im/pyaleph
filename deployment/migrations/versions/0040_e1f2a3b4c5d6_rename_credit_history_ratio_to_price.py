"""rename credit_history ratio column to price

Revision ID: e1f2a3b4c5d6
Revises: d0e1f2a3b4c5
Create Date: 2025-12-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e1f2a3b4c5d6'
down_revision = 'd0e1f2a3b4c5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # First, add the new price column
    op.add_column('credit_history', sa.Column('price', sa.DECIMAL(), nullable=True))

    # Add the new bonus_amount column
    op.add_column('credit_history', sa.Column('bonus_amount', sa.BigInteger(), nullable=True))

    # Transform data: price calculation depends on payment token
    # For ALEPH token: ratio = (1 / price) * (1 + 0.2), therefore price = 1.2 / ratio
    # For other tokens: price = 1/ratio
    # Only update rows where payment_method is NOT 'credit_expense' or 'credit_transfer'
    # and where ratio is not null and not zero
    connection = op.get_bind()
    connection.execute(sa.text("""
        UPDATE credit_history
        SET price = CASE
            WHEN token = 'ALEPH' THEN ROUND(1.2 / ratio, 18)
            ELSE ROUND(1.0 / ratio, 18)
        END
        WHERE ratio IS NOT NULL
        AND ratio != 0
        AND (payment_method IS NULL
             OR (payment_method != 'credit_expense' AND payment_method != 'credit_transfer'))
    """))

    # Update bonus_amount for ALEPH token records
    connection.execute(sa.text("""
        UPDATE credit_history
        SET bonus_amount = TRUNC(amount * 0.20)
        WHERE token = 'ALEPH'
        AND amount IS NOT NULL
    """))

    # Drop the old ratio column
    op.drop_column('credit_history', 'ratio')


def downgrade() -> None:
    # Add back the ratio column
    op.add_column('credit_history', sa.Column('ratio', sa.DECIMAL(), nullable=True))

    # Transform data back: reverse price calculation depends on payment token
    # For ALEPH token: price = 1.2 / ratio, therefore ratio = 1.2 / price
    # For other tokens: ratio = 1/price
    connection = op.get_bind()
    connection.execute(sa.text("""
        UPDATE credit_history
        SET ratio = CASE
            WHEN token = 'ALEPH' THEN ROUND(1.2 / price, 18)
            ELSE ROUND(1.0 / price, 18)
        END
        WHERE price IS NOT NULL
        AND price != 0
        AND (payment_method IS NULL
             OR (payment_method != 'credit_expense' AND payment_method != 'credit_transfer'))
    """))

    # Drop the price column
    op.drop_column('credit_history', 'price')

    # Drop the bonus_amount column
    op.drop_column('credit_history', 'bonus_amount')