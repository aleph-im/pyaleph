"""credit ledger v2 tables

Revision ID: 5b7e1c9a3d46
Revises: d2f4a7c9e1b3
Create Date: 2026-10-20 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import func

revision = "5b7e1c9a3d46"
down_revision = "d2f4a7c9e1b3"
branch_labels = None
depends_on = None

TS = sa.TIMESTAMP(timezone=True)


def upgrade() -> None:
    op.create_table(
        "billing_resources",
        sa.Column("item_hash", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("payment_type", sa.String(), nullable=False),
        sa.Column("first_confirmed_at", TS, nullable=True),
        sa.Column("end_of_life", TS, nullable=True),
        sa.Column(
            "end_of_life_provisional",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("end_reason", sa.String(), nullable=True),
        sa.Column("cost_spec", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", TS, nullable=False, server_default=func.now()),
        sa.PrimaryKeyConstraint("item_hash", name="billing_resources_pkey"),
    )
    op.create_index(
        "ix_billing_resources_owner_payment_type",
        "billing_resources",
        ["owner", "payment_type"],
    )
    op.create_index(
        "ix_billing_resources_type_payment_type_first_confirmed_at",
        "billing_resources",
        ["type", "payment_type", "first_confirmed_at"],
    )
    op.create_index(
        "ix_billing_resources_end_of_life", "billing_resources", ["end_of_life"]
    )

    op.create_table(
        "billing_rates",
        sa.Column("item_hash", sa.String(), nullable=False),
        sa.Column("pricing_version", sa.String(), nullable=False),
        sa.Column("rate_credit", sa.DECIMAL(40, 18), nullable=False),
        sa.PrimaryKeyConstraint(
            "item_hash", "pricing_version", name="billing_rates_pkey"
        ),
    )

    op.create_table(
        "credit_ledger",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("period", TS, nullable=False),
        sa.Column("address", sa.String(), nullable=True),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("business_key", sa.String(), nullable=False),
        sa.Column("amount", sa.BigInteger(), nullable=False),
        sa.Column("resource", sa.String(), nullable=True),
        sa.Column("node_id", sa.String(), nullable=True),
        sa.Column("seconds", sa.Integer(), nullable=True),
        sa.Column("count", sa.Integer(), nullable=True),
        sa.Column("size_mib", sa.Integer(), nullable=True),
        sa.Column("rate", sa.DECIMAL(40, 18), nullable=True),
        sa.Column("expiration", TS, nullable=True),
        sa.Column("source_ref", sa.String(), nullable=True),
        sa.Column("excluded_reason", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="credit_ledger_pkey"),
        sa.UniqueConstraint("kind", "business_key", name="uq_credit_ledger_kind_key"),
    )
    op.create_index(
        "ix_credit_ledger_address_period", "credit_ledger", ["address", "period"]
    )
    op.create_index("ix_credit_ledger_period_kind", "credit_ledger", ["period", "kind"])
    op.create_index("ix_credit_ledger_resource", "credit_ledger", ["resource"])
    op.create_index(
        "ix_credit_ledger_node_id_period", "credit_ledger", ["node_id", "period"]
    )

    op.create_table(
        "credit_lots",
        sa.Column("ledger_id", sa.BigInteger(), nullable=False),
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("period", TS, nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("business_key", sa.String(), nullable=False),
        sa.Column("amount_remaining", sa.BigInteger(), nullable=False),
        sa.Column("expiration", TS, nullable=True),
        sa.ForeignKeyConstraint(["ledger_id"], ["credit_ledger.id"]),
        sa.PrimaryKeyConstraint("ledger_id", name="credit_lots_pkey"),
    )
    op.create_index(
        "ix_credit_lots_address_order",
        "credit_lots",
        ["address", "period", "kind", "business_key"],
    )
    op.create_index(
        "ix_credit_lots_address_active",
        "credit_lots",
        ["address"],
        postgresql_where=sa.text("amount_remaining > 0"),
    )

    op.create_table(
        "credit_checkpoints",
        sa.Column("period", TS, nullable=False),
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("lots", postgresql.JSONB(), nullable=False),
        sa.Column("ledger_hash", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("period", "address", name="credit_checkpoints_pkey"),
    )

    op.create_table(
        "credit_topups",
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("tx_hash", sa.String(), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("block_number", sa.BigInteger(), nullable=False),
        sa.Column("block_time", TS, nullable=False),
        sa.Column("from_address", sa.String(), nullable=False),
        sa.Column("token", sa.String(), nullable=False),
        sa.Column("raw_amount", sa.NUMERIC(78, 0), nullable=False),
        sa.Column("quote_num", sa.NUMERIC(78, 0), nullable=True),
        sa.Column("quote_den", sa.NUMERIC(78, 0), nullable=True),
        sa.Column("credits", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("ledger_period", TS, nullable=True),
        sa.PrimaryKeyConstraint(
            "chain_id", "tx_hash", "log_index", name="credit_topups_pkey"
        ),
    )

    op.create_table(
        "credit_attestations",
        sa.Column("item_hash", sa.String(), nullable=False),
        sa.Column("period", TS, nullable=False),
        sa.Column("sender", sa.String(), nullable=False),
        sa.Column("supersedes", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("received_at", TS, nullable=False, server_default=func.now()),
        sa.PrimaryKeyConstraint("item_hash", name="credit_attestations_pkey"),
    )
    op.create_index(
        "ux_credit_attestations_active_period",
        "credit_attestations",
        ["period"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )

    op.create_table(
        "credit_fold_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("last_closed_period", TS, nullable=True),
        sa.Column("dirty_from_period", TS, nullable=True),
        sa.Column("chain_cursor_block", sa.BigInteger(), nullable=True),
        sa.Column("chain_cursor_time", TS, nullable=True),
        sa.Column("genesis_loaded_hash", sa.String(), nullable=True),
        sa.Column(
            "backfill_done", sa.Boolean(), nullable=False, server_default="false"
        ),
        sa.PrimaryKeyConstraint("id", name="credit_fold_state_pkey"),
    )

    op.create_table(
        "credit_period_quotes",
        sa.Column("period", TS, nullable=False),
        sa.Column("aleph_per_credit", sa.NUMERIC(40, 18), nullable=False),
        sa.Column("block_number", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("period", name="credit_period_quotes_pkey"),
    )

    op.execute(text("INSERT INTO credit_fold_state(id) VALUES (1)"))
    # 300 s is a floor only: the cron runner ticks every aleph.jobs.cron.period
    # (30 min by default), so the fold actually runs every 30 min.
    op.execute(
        text(
            """
        INSERT INTO cron_jobs(id, interval, last_run)
        VALUES ('credit_fold', 300, '2025-01-01 00:00:00')
        """
        )
    )


def downgrade() -> None:
    op.execute(text("DELETE FROM cron_jobs WHERE id = 'credit_fold'"))
    op.drop_table("credit_period_quotes")
    op.drop_table("credit_fold_state")
    op.drop_index(
        "ux_credit_attestations_active_period", table_name="credit_attestations"
    )
    op.drop_table("credit_attestations")
    op.drop_table("credit_topups")
    op.drop_table("credit_checkpoints")
    op.drop_index("ix_credit_lots_address_active", table_name="credit_lots")
    op.drop_index("ix_credit_lots_address_order", table_name="credit_lots")
    op.drop_table("credit_lots")
    op.drop_index("ix_credit_ledger_node_id_period", table_name="credit_ledger")
    op.drop_index("ix_credit_ledger_resource", table_name="credit_ledger")
    op.drop_index("ix_credit_ledger_period_kind", table_name="credit_ledger")
    op.drop_index("ix_credit_ledger_address_period", table_name="credit_ledger")
    op.drop_table("credit_ledger")
    op.drop_table("billing_rates")
    op.drop_index("ix_billing_resources_end_of_life", table_name="billing_resources")
    op.drop_index(
        "ix_billing_resources_type_payment_type_first_confirmed_at",
        table_name="billing_resources",
    )
    op.drop_index(
        "ix_billing_resources_owner_payment_type", table_name="billing_resources"
    )
    op.drop_table("billing_resources")
