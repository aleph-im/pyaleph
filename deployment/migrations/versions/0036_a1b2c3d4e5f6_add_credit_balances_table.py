"""add credit_balances table

Revision ID: a1b2c3d4e5f6
Revises: 35a67ccc4451
Create Date: 2025-07-14 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = '35a67ccc4451'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create credit_balances table
    op.create_table(
        'credit_balances',
        sa.Column('id', sa.BigInteger(), autoincrement=True),
        sa.Column('address', sa.String(), nullable=False),
        sa.Column('amount', sa.BigInteger(), nullable=False),
        sa.Column('ratio', sa.DECIMAL(), nullable=True),
        sa.Column('tx_hash', sa.String(), nullable=True),
        sa.Column('token', sa.String(), nullable=True),
        sa.Column('chain', sa.String(), nullable=True),
        sa.Column('provider', sa.String(), nullable=True),
        sa.Column('origin', sa.String(), nullable=True),
        sa.Column('origin_ref', sa.String(), nullable=True),
        sa.Column('payment_method', sa.String(), nullable=True),
        sa.Column('credit_ref', sa.String(), nullable=False),
        sa.Column('credit_index', sa.Integer(), nullable=False),
        sa.Column('expiration_date', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('last_update', sa.TIMESTAMP(timezone=True), nullable=False, 
                  server_default=func.now(), onupdate=func.now()),
        sa.PrimaryKeyConstraint('credit_ref', 'credit_index', name='credit_balances_pkey'),
    )
    
    # Create index on address for efficient lookups
    op.create_index(op.f('ix_credit_balances_address'), 'credit_balances', ['address'], unique=False)
    
    # Add unique constraint on tx_hash to prevent duplicate credit lines (when tx_hash is not null)
    op.create_unique_constraint('credit_balances_tx_hash_key', 'credit_balances', ['tx_hash'])


def downgrade() -> None:
    # Drop the credit_balances table and its constraints
    op.drop_index('ix_credit_balances_address', 'credit_balances')
    op.drop_constraint('credit_balances_tx_hash_key', 'credit_balances')
    op.drop_table('credit_balances')