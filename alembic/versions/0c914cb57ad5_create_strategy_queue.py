"""create strategy queue

Revision ID: 0c914cb57ad5
Revises: b94b3b80997e
Create Date: 2022-01-24 18:34:44.455206

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0c914cb57ad5'
down_revision = 'b94b3b80997e'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "strategy_queue",
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_strategy_queue')),
        sa.Column('timestamp', sa.TIMESTAMP()),
        sa.Column('strategy', sa.String()),
        sa.Column('quote', sa.String()),
        sa.Column('base', sa.String()),
        sa.Column('exchange', sa.String()),
        sa.Column('product_type', sa.String()),
        sa.Column('group', sa.String()),
        sa.Column('relative_size', sa.Float()),
        sa.Column('processed_timestamp', sa.String()),
    )


def downgrade():
    op.drop_table("strategy_queue")
