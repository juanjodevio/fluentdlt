"""create events table

Revision ID: 002
Revises: 001
Create Date: 2025-11-14

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create events table for testing incremental loading with timestamps."""
    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("event_data", sa.String(500), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )

    # Create indexes for incremental loading and foreign key
    op.create_index("idx_events_created_at", "events", ["created_at"])
    op.create_index("idx_events_user_id", "events", ["user_id"])


def downgrade() -> None:
    """Drop events table."""
    op.drop_index("idx_events_user_id", table_name="events")
    op.drop_index("idx_events_created_at", table_name="events")
    op.drop_table("events")
