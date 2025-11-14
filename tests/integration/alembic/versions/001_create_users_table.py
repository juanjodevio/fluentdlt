"""create users table

Revision ID: 001
Revises:
Create Date: 2025-11-14

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create users table for testing SQL table loading and incremental updates."""
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("email", sa.String(100), nullable=False),
        sa.Column("age", sa.Integer(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create index on updated_at for incremental loading performance
    op.create_index("idx_users_updated_at", "users", ["updated_at"])


def downgrade() -> None:
    """Drop users table."""
    op.drop_index("idx_users_updated_at", table_name="users")
    op.drop_table("users")
