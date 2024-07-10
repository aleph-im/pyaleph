"""message content in file pins

Revision ID: daa92b500049
Revises: 7ab62bd0a3b1
Create Date: 2023-04-12 14:33:55.891990

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "daa92b500049"
down_revision = "7ab62bd0a3b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # We now store a file + file pin object for the content of each non-inline message.

    # The existing unique constraint on item_hash will fail because of some non-inline STORE messages.
    op.drop_constraint("file_pins_item_hash_key", "file_pins", type_="unique")
    op.create_unique_constraint(
        "file_pins_item_hash_type_key", "file_pins", ["item_hash", "type"]
    )

    op.execute(
        """
        INSERT INTO files(hash, size, type) 
            SELECT messages.item_hash, messages.size, 'file' 
            FROM messages WHERE item_type != 'inline'
        """
    )
    op.execute(
        """
        INSERT INTO file_pins(file_hash, created, type, tx_hash, owner, item_hash, ref)
        SELECT  messages.item_hash,
                to_timestamp((messages.content ->> 'time')::float),
                'content',
                null,
                messages.sender,
                messages.item_hash,
                null
        FROM messages
        WHERE item_type != 'inline'
        """
    )


def downgrade() -> None:
    op.execute("DELETE FROM file_pins WHERE type = 'content'")
    op.execute(
        "DELETE FROM files WHERE EXISTS (SELECT 1 FROM messages WHERE messages.item_hash = hash)"
    )
    op.drop_constraint("file_pins_item_hash_type_key", "file_pins", type_="unique")
    op.create_unique_constraint("file_pins_item_hash_key", "file_pins", ["item_hash"])
