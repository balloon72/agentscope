"""Durable session run leases and fencing tokens.

Revision ID: 0005_session_fencing
Revises: 0004_sops
Create Date: 2026-10-16 00:00:00.000000

The live message-bus lock is intentionally ephemeral.  These columns make
the ownership decision durable so a worker that lost a lease cannot overwrite
state after another replica has taken over the session.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0005_session_fencing"
down_revision: Union[str, None] = "0004_sops"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add nullable lease columns and backwards-compatible defaults."""
    with op.batch_alter_table("sessions", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("lease_owner", sa.String(length=255), nullable=True),
        )
        batch_op.add_column(
            sa.Column(
                "lease_epoch",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
        )
        batch_op.add_column(
            sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        )
        batch_op.add_column(
            sa.Column(
                "state_version",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
        )
        batch_op.add_column(
            sa.Column(
                "run_status",
                sa.String(length=32),
                nullable=False,
                server_default="idle",
            ),
        )
        for column in ("lease_owner", "lease_expires_at", "run_status"):
            batch_op.create_index(
                batch_op.f(f"ix_sessions_{column}"),
                [column],
                unique=False,
            )


def downgrade() -> None:
    """Remove the durable fencing columns."""
    with op.batch_alter_table("sessions", schema=None) as batch_op:
        for column in ("run_status", "lease_expires_at", "lease_owner"):
            batch_op.drop_index(batch_op.f(f"ix_sessions_{column}"))
        for column in (
            "run_status",
            "state_version",
            "lease_expires_at",
            "lease_epoch",
            "lease_owner",
        ):
            batch_op.drop_column(column)
