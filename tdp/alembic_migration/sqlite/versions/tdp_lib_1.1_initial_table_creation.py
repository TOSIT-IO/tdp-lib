# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""Initial table creation

Revision ID: tdp_lib_1.1
Revises:
Create Date: 2024-06-11 09:59:29.908273

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "tdp_lib_1.1"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "deployment",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("options", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("start_time", sa.DateTime(), nullable=True),
        sa.Column("end_time", sa.DateTime(), nullable=True),
        sa.Column(
            "state",
            sa.Enum(
                "PLANNED", "RUNNING", "SUCCESS", "FAILURE", name="deploymentstateenum"
            ),
            nullable=True,
        ),
        sa.Column(
            "deployment_type",
            sa.Enum(
                "DAG",
                "OPERATIONS",
                "RESUME",
                "RECONFIGURE",
                "CUSTOM",
                name="deploymenttypeenum",
            ),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "operation",
        sa.Column("deployment_id", sa.Integer(), nullable=False),
        sa.Column("operation_order", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(length=72), nullable=False),
        sa.Column("host", sa.String(length=255), nullable=True),
        sa.Column("extra_vars", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("start_time", sa.DateTime(), nullable=True),
        sa.Column("end_time", sa.DateTime(), nullable=True),
        sa.Column(
            "state",
            sa.Enum(
                "PLANNED",
                "RUNNING",
                "PENDING",
                "SUCCESS",
                "FAILURE",
                "HELD",
                name="operationstateenum",
            ),
            nullable=False,
        ),
        sa.Column("logs", sa.LargeBinary(length=10000000), nullable=True),
        sa.ForeignKeyConstraint(
            ["deployment_id"],
            ["deployment.id"],
        ),
        sa.PrimaryKeyConstraint("deployment_id", "operation_order"),
    )
    op.create_table(
        "sch_status_log",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_time", sa.DateTime(), nullable=False),
        sa.Column("service", sa.String(length=20), nullable=False),
        sa.Column("component", sa.String(length=30), nullable=True),
        sa.Column("host", sa.String(length=255), nullable=True),
        sa.Column("running_version", sa.String(length=40), nullable=True),
        sa.Column("configured_version", sa.String(length=40), nullable=True),
        sa.Column("to_config", sa.Boolean(), nullable=True),
        sa.Column("to_restart", sa.Boolean(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=True),
        sa.Column(
            "source",
            sa.Enum(
                "DEPLOYMENT", "FORCED", "STALE", "MANUAL", name="schstatuslogsourceenum"
            ),
            nullable=False,
        ),
        sa.Column("deployment_id", sa.Integer(), nullable=True),
        sa.Column("message", sa.String(length=512), nullable=True),
        sa.ForeignKeyConstraint(
            ["deployment_id"],
            ["deployment.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###
