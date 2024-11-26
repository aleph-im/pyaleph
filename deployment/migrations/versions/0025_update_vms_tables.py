"""
Update vms tables

Revision ID: 63b767213bfa
Revises: 8b27064157d7
Create Date: 2024-11-19 14:30:13.877818

"""

import asyncio
import json
from threading import Thread

import sqlalchemy as sa
from alembic import op
from sqlalchemy import column, table, text

revision = "63b767213bfa"
down_revision = "8b27064157d7"
branch_labels = None
depends_on = None


async def update_payment_types() -> None:
    """
    Update the `payment_type` column in the `vms` table based on the `messages` table.
    """
    conn = op.get_bind()

    vms_table = table(
        "vms", column("item_hash", sa.String), column("payment_type", sa.String)
    )

    query = text(
        """
        SELECT 
            vms.item_hash AS vm_item_hash,
            messages.item_content AS message_content
        FROM 
            vms
        LEFT JOIN 
            messages ON vms.item_hash = messages.item_hash
    """
    )

    rows = conn.execute(query).fetchall()

    for row in rows:
        vm_item_hash = row["vm_item_hash"]
        message_content = row["message_content"]

        payment_type = "hold"

        if message_content:
            message_data = json.loads(message_content)

            payment = message_data.get("payment")
            if payment:
                payment_type = payment.get("type", "hold")

        conn.execute(
            vms_table.update()
            .where(vms_table.c.item_hash == vm_item_hash)
            .values(payment_type=payment_type)
        )


def upgrade_thread():
    asyncio.run(update_payment_types())


def upgrade() -> None:
    thread = Thread(target=upgrade_thread, daemon=True)
    thread.start()
    thread.join()


def downgrade() -> None:
    pass
