"""Add expression index account_costs (lower(owner))

Revision ID: c3e8a1f5b7d2
Revises: 5b7e1c9a3d46
Create Date: 2026-10-20

Serving the v2 credit ledger, accounts are keyed by the lowercased EVM
address while account_costs keeps the owner as each message carried it. The
admission check and the credit removal cron match owners case-insensitively
(`lower(owner) = :address`), which this index serves instead of a sequential
scan of account_costs per account.
"""

from alembic import op
from sqlalchemy import text

revision = "c3e8a1f5b7d2"
down_revision = "5b7e1c9a3d46"
branch_labels = None
depends_on = None


def _run_outside_transaction(statement: str) -> None:
    connection = op.get_bind()

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    with connection.engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(statement))

    if was_in_transaction:
        connection.execute(text("BEGIN"))


def upgrade() -> None:
    _run_outside_transaction(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_account_costs_owner_lower "
        "ON account_costs (lower(owner))"
    )


def downgrade() -> None:
    _run_outside_transaction(
        "DROP INDEX CONCURRENTLY IF EXISTS ix_account_costs_owner_lower"
    )
