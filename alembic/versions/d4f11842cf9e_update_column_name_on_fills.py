"""update column name on fills

Revision ID: d4f11842cf9e
Revises: 582295d811a9
Create Date: 2022-02-09 23:12:43.320684

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd4f11842cf9e'
down_revision = '582295d811a9'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('fills', 'unfilled_quantity_quote', nullable=False, new_column_name='unfilled_quantity')
    op.alter_column('fills', 'order_quantity_quote', nullable=False, new_column_name='order_quantity')
    op.add_column('fills', sa.Column('quantity_type', sa.String()))


def downgrade():
    op.alter_column('fills', 'unfilled_quantity', nullable=False, new_column_name='unfilled_quantity_quote')
    op.alter_column('fills', 'order_quantity', nullable=False, new_column_name='order_quantity_quote')
    op.drop_column('fills', 'quantity_type')

