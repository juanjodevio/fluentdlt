"""seed test data

Revision ID: 004
Revises: 003
Create Date: 2025-11-14

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Insert synthetic test data into all tables."""
    
    # Insert users (5 users with sequential IDs and dates)
    op.execute(
        """
        INSERT INTO users (id, name, email, age, is_active, updated_at) VALUES
        (1, 'Alice', 'alice@example.com', 28, 1, '2024-01-01 10:00:00'),
        (2, 'Bob', 'bob@example.com', 35, 1, '2024-01-02 10:00:00'),
        (3, 'Charlie', 'charlie@example.com', 42, 0, '2024-01-03 10:00:00'),
        (4, 'Diana', 'diana@example.com', 31, 1, '2024-01-04 10:00:00'),
        (5, 'Eve', 'eve@example.com', 29, 1, '2024-01-05 10:00:00')
        """
    )
    
    # Insert events (10 events with various types and timestamps)
    op.execute(
        """
        INSERT INTO events (id, user_id, event_type, event_data, created_at) VALUES
        (1, 1, 'login', 'IP: 192.168.1.1', '2024-01-01 09:00:00'),
        (2, 1, 'purchase', 'Product: Widget, Amount: 25.00', '2024-01-01 11:00:00'),
        (3, 2, 'login', 'IP: 192.168.1.2', '2024-01-02 08:30:00'),
        (4, 3, 'login', 'IP: 192.168.1.3', '2024-01-03 07:15:00'),
        (5, 1, 'logout', NULL, '2024-01-03 15:00:00'),
        (6, 4, 'login', 'IP: 192.168.1.4', '2024-01-04 10:20:00'),
        (7, 2, 'purchase', 'Product: Gadget, Amount: 50.00', '2024-01-04 14:00:00'),
        (8, 5, 'login', 'IP: 192.168.1.5', '2024-01-05 09:45:00'),
        (9, 4, 'purchase', 'Product: Widget, Amount: 25.00', '2024-01-05 16:30:00'),
        (10, 5, 'logout', NULL, '2024-01-05 18:00:00')
        """
    )
    
    # Insert products (3 products in different categories)
    op.execute(
        """
        INSERT INTO products (id, name, category, price, stock, created_at) VALUES
        (1, 'Widget', 'Electronics', 25.00, 100, '2024-01-01 00:00:00'),
        (2, 'Gadget', 'Electronics', 50.00, 50, '2024-01-01 00:00:00'),
        (3, 'Tool', 'Hardware', 15.00, 200, '2024-01-01 00:00:00')
        """
    )


def downgrade() -> None:
    """Remove test data (optional - tables will be dropped anyway)."""
    op.execute("DELETE FROM events")
    op.execute("DELETE FROM products")
    op.execute("DELETE FROM users")

