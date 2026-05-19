"""Persist node scoring metrics

Revision ID: b9c4f1e6a2d7
Revises: 4f1e8d2a6c3b
Create Date: 2026-05-18

Creates crn_metrics and ccn_metrics tables, backfills them from the
existing JSON-unnesting views, then drops the views.

Backfill ordering: tables are created without indexes or FK, the bulk
INSERT runs against bare heap tables, then indexes are built in one
shot on the populated tables (sorted bulk build) and the FK is added
NOT VALID + VALIDATE-d separately. This avoids per-row index
maintenance and per-row FK lookups against messages during the
backfill, which fans out to roughly scoring_messages * nodes_per_msg
rows.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision = "b9c4f1e6a2d7"
down_revision = "4f1e8d2a6c3b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crn_metrics",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("item_hash", sa.String, nullable=False),
        sa.Column("node_id", sa.String, nullable=False),
        sa.Column("measured_at", sa.Float, nullable=False),
        sa.Column("base_latency", sa.Float),
        sa.Column("base_latency_ipv4", sa.Float),
        sa.Column("full_check_latency", sa.Float),
        sa.Column("diagnostic_vm_latency", sa.Float),
    )
    op.create_table(
        "ccn_metrics",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("item_hash", sa.String, nullable=False),
        sa.Column("node_id", sa.String, nullable=False),
        sa.Column("measured_at", sa.Float, nullable=False),
        sa.Column("base_latency", sa.Float),
        sa.Column("base_latency_ipv4", sa.Float),
        sa.Column("metrics_latency", sa.Float),
        sa.Column("aggregate_latency", sa.Float),
        sa.Column("file_download_latency", sa.Float),
        sa.Column("pending_messages", sa.Integer),
        sa.Column("eth_height_remaining", sa.Integer),
    )

    op.execute(
        text(
            """
        INSERT INTO crn_metrics (
            item_hash, node_id, measured_at, base_latency, base_latency_ipv4,
            full_check_latency, diagnostic_vm_latency
        )
        SELECT
            item_hash, node_id, measured_at, base_latency, base_latency_ipv4,
            full_check_latency, diagnostic_vm_latency
        FROM crn_metric_view
        WHERE node_id IS NOT NULL AND measured_at IS NOT NULL
        """
        )
    )
    op.execute(
        text(
            """
        INSERT INTO ccn_metrics (
            item_hash, node_id, measured_at, base_latency, base_latency_ipv4,
            metrics_latency, aggregate_latency, file_download_latency,
            pending_messages, eth_height_remaining
        )
        SELECT
            item_hash, node_id, measured_at, base_latency, base_latency_ipv4,
            metrics_latency, aggregate_latency, file_download_latency,
            pending_messages, eth_height_remaining
        FROM ccn_metric_view
        WHERE node_id IS NOT NULL AND measured_at IS NOT NULL
        """
        )
    )

    op.create_index("ix_crn_metrics_item_hash", "crn_metrics", ["item_hash"])
    op.create_index(
        "ix_crn_metrics_node_id_measured_at",
        "crn_metrics",
        ["node_id", sa.text("measured_at DESC")],
    )
    op.create_index("ix_ccn_metrics_item_hash", "ccn_metrics", ["item_hash"])
    op.create_index(
        "ix_ccn_metrics_node_id_measured_at",
        "ccn_metrics",
        ["node_id", sa.text("measured_at DESC")],
    )

    op.execute(
        text(
            """
        ALTER TABLE crn_metrics
          ADD CONSTRAINT fk_crn_metrics_item_hash
          FOREIGN KEY (item_hash) REFERENCES messages(item_hash) ON DELETE CASCADE
          NOT VALID
        """
        )
    )
    op.execute(
        text(
            """
        ALTER TABLE ccn_metrics
          ADD CONSTRAINT fk_ccn_metrics_item_hash
          FOREIGN KEY (item_hash) REFERENCES messages(item_hash) ON DELETE CASCADE
          NOT VALID
        """
        )
    )
    op.execute(
        text("ALTER TABLE crn_metrics VALIDATE CONSTRAINT fk_crn_metrics_item_hash")
    )
    op.execute(
        text("ALTER TABLE ccn_metrics VALIDATE CONSTRAINT fk_ccn_metrics_item_hash")
    )

    op.execute(text("DROP VIEW IF EXISTS crn_metric_view"))
    op.execute(text("DROP VIEW IF EXISTS ccn_metric_view"))


def downgrade() -> None:
    op.execute(
        text(
            """
        CREATE OR REPLACE VIEW ccn_metric_view AS
        WITH json_data AS (
            SELECT item_hash,
                   jsonb_array_elements(content -> 'content' -> 'metrics' -> 'ccn') as ccn_data
            FROM messages
            WHERE channel = 'aleph-scoring'
              AND sender = '0x4D52380D3191274a04846c89c069E6C3F2Ed94e4'
        )
        SELECT item_hash,
               (ccn_data ->> 'measured_at')::float           as measured_at,
               ccn_data ->> 'node_id'                        as node_id,
               (ccn_data ->> 'base_latency')::float          as base_latency,
               (ccn_data ->> 'metrics_latency')::float       as metrics_latency,
               (ccn_data ->> 'aggregate_latency')::float     as aggregate_latency,
               (ccn_data ->> 'base_latency_ipv4')::float     as base_latency_ipv4,
               (ccn_data ->> 'file_download_latency')::float as file_download_latency,
               (ccn_data ->> 'pending_messages')::int        as pending_messages,
               (ccn_data ->> 'eth_height_remaining')::int    as eth_height_remaining
        FROM json_data
        """
        )
    )
    op.execute(
        text(
            """
        CREATE OR REPLACE VIEW crn_metric_view AS
        WITH json_data AS (
            SELECT item_hash,
                   jsonb_array_elements(content -> 'content' -> 'metrics' -> 'crn') as crn_data
            FROM messages
            WHERE channel = 'aleph-scoring'
              AND sender = '0x4D52380D3191274a04846c89c069E6C3F2Ed94e4'
        )
        SELECT item_hash                                       as item_hash,
               (crn_data ->> 'measured_at')::float             as measured_at,
               crn_data ->> 'node_id'                          as node_id,
               (crn_data ->> 'base_latency')::float            as base_latency,
               (crn_data ->> 'base_latency_ipv4')::float       as base_latency_ipv4,
               (crn_data ->> 'full_check_latency')::float      as full_check_latency,
               (crn_data ->> 'diagnostic_vm_latency')::float   as diagnostic_vm_latency
        FROM json_data
        """
        )
    )
    op.drop_index("ix_ccn_metrics_node_id_measured_at", table_name="ccn_metrics")
    op.drop_index("ix_ccn_metrics_item_hash", table_name="ccn_metrics")
    op.drop_table("ccn_metrics")
    op.drop_index("ix_crn_metrics_node_id_measured_at", table_name="crn_metrics")
    op.drop_index("ix_crn_metrics_item_hash", table_name="crn_metrics")
    op.drop_table("crn_metrics")
