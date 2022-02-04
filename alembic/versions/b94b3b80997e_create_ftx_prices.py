"""create ftx prices

Revision ID: b94b3b80997e
Revises: 
Create Date: 2022-01-13 09:05:42.433027

"""
import alembic
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b94b3b80997e'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    alembic.op.create_table(
        "price_data",
        sa.Column('timestamp', sa.TIMESTAMP()),
        sa.Column('exchange', sa.String()),
        sa.Column('fetch_time', sa.TIMESTAMP()),
        sa.Column('resolution', sa.Integer()),
        sa.Column('open', sa.Float()),
        sa.Column('high', sa.Float()),
        sa.Column('low', sa.Float()),
        sa.Column('close', sa.Float()),
        sa.Column('quote_volume', sa.Float()),
        sa.Column('quote', sa.String()),
        sa.Column('base', sa.String()),
        sa.Column('product_type', sa.String()),
        sa.Column('expiry_date', sa.String())
    )

    alembic.op.create_table('funding_data',
        sa.Column('timestamp', sa.TIMESTAMP()),
        sa.Column('future', sa.String()),
        sa.Column('exchange', sa.String()),
        sa.Column('fetch_time', sa.TIMESTAMP()),
        sa.Column('funding_rate', sa.Float()),
        sa.Column('symbol', sa.String())
    )


def downgrade():
    alembic.op.drop_table("price_data")
    alembic.op.drop_table("funding_data")