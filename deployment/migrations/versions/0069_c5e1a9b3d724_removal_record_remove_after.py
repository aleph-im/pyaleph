"""Persist the removal grace deadline on the removal record

Revision ID: c5e1a9b3d724
Revises: d2f4a7c9e1b3
Create Date: 2026-09-30

The balance/credit-balance crons flip a message PROCESSED->REMOVING when the
account can no longer fund one more day of it, and flip it back to PROCESSED
once the balance recovers. REMOVING is therefore a reversible state, and the
garbage collector is what makes it permanent (REMOVING->REMOVED deletes the
messages row, leaving only this snapshot).

That reversible window only ever existed for STORE messages: the crons stamp a
grace period on the file pin, and the collector refuses to finalize while the
pin is alive. Every other type (INSTANCE, PROGRAM, V-PROGRAM) pins no file, so
the collector finalized it on its very next pass -- between 0 and
garbage_collector_period hours after the flip, depending only on where in the
collection cycle the cron happened to run. Those are the credit-funded
resources, so an expense-message backlog (a node catching up after an outage)
could drain many accounts at once and permanently remove their workloads with
no practical window to top up.

The crons already compute that deadline for every type; it was simply not
persisted for the ones that pin no file. This migration adds the column so the
collector can honour it uniformly.

Rows still in REMOVING (removed_at IS NULL) are backfilled with a fresh
deadline rather than left NULL, so removals already in flight when this
migration runs get the grace window too instead of being finalized on the next
collection. A NULL deadline stays eligible for immediate finalization: legacy
REMOVING messages predating the snapshot record have no row here at all, and
must not be stranded in REMOVING forever.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c5e1a9b3d724"
down_revision = "d2f4a7c9e1b3"
branch_labels = None
depends_on = None

# Matches the grace period the crons apply to STORE file pins (24h + 1h of
# slack so a collection landing exactly on the boundary does not race it).
GRACE_PERIOD_HOURS = 24 + 1


def upgrade() -> None:
    op.add_column(
        "removed_messages",
        sa.Column("remove_after", sa.TIMESTAMP(timezone=True), nullable=True),
    )

    # Give in-flight removals a full grace window from this migration onwards.
    op.execute(
        sa.text(
            """
        UPDATE removed_messages
        SET remove_after = now() + make_interval(hours => :grace_hours)
        WHERE removed_at IS NULL
        """
        ).bindparams(grace_hours=GRACE_PERIOD_HOURS)
    )


def downgrade() -> None:
    op.drop_column("removed_messages", "remove_after")
