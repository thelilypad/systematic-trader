"""create performance attribution table(s)

Revision ID: 582295d811a9
Revises: 0c914cb57ad5
Create Date: 2022-02-07 22:42:03.917220

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '582295d811a9'
down_revision = '0c914cb57ad5'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "performance_attribution",
        sa.Column('timestamp', sa.TIMESTAMP()),
        sa.Column('strategy', sa.String()),
        sa.Column('quote', sa.String()),
        sa.Column('base', sa.String()),
        sa.Column('exchange', sa.String()),
        sa.Column('product_type', sa.String()),
        sa.Column('group', sa.String()),
        sa.Column('position_value', sa.Float()),
    )

    op.create_table(
        "fills",
        sa.Column('quote', sa.String()),
        sa.Column('base', sa.String()),
        sa.Column('exchange', sa.String()),
        sa.Column('product_type', sa.String()),
        sa.Column('best_price', sa.Float()),
        sa.Column('fill_average_price', sa.Float()),
        sa.Column('slippage_ratio', sa.Float()),
        sa.Column('fill_json', sa.String()),
        sa.Column('order_quantity_quote', sa.String()),
        sa.Column('order_type', sa.String()),
        sa.Column('unfilled_quantity_quote', sa.String()),
        sa.Column('start_timestamp', sa.Numeric()),
        sa.Column('end_timestamp', sa.Numeric()),
    )


def downgrade():
    op.drop_table("fills")
    op.drop_table("performance_attribution")