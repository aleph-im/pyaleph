"""add credit_history and credit_balances tables

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
    # Create credit_history table (detailed audit trail)
    op.create_table(
        'credit_history',
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
        sa.Column('message_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('last_update', sa.TIMESTAMP(timezone=True), nullable=False, 
                  server_default=func.now(), onupdate=func.now()),
        sa.PrimaryKeyConstraint('credit_ref', 'credit_index', name='credit_history_pkey'),
    )
    
    # Create indexes on credit_history for efficient lookups
    op.create_index('ix_credit_history_address', 'credit_history', ['address'], unique=False)
    op.create_index('ix_credit_history_message_timestamp', 'credit_history', ['message_timestamp'], unique=False)
    
    # Create credit_balances table (cached balance summary)
    op.create_table(
        'credit_balances',
        sa.Column('address', sa.String(), nullable=False),
        sa.Column('balance', sa.BigInteger(), nullable=False, default=0),
        sa.Column('last_update', sa.TIMESTAMP(timezone=True), nullable=False, 
                  server_default=func.now(), onupdate=func.now()),
        sa.PrimaryKeyConstraint('address', name='credit_balances_pkey'),
    )
    
    # Create index on address for the cached balances table
    op.create_index('ix_credit_balances_address', 'credit_balances', ['address'], unique=False)


def downgrade() -> None:
    # Drop the credit_balances table
    op.drop_index('ix_credit_balances_address', 'credit_balances')
    op.drop_table('credit_balances')
    
    # Drop the credit_history table
    op.drop_index('ix_credit_history_address', 'credit_history')
    op.drop_index('ix_credit_history_message_timestamp', 'credit_history')
    op.drop_table('credit_history')