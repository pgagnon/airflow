#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Add task instance anchor to deadline and task_id to deadline_alert.

Revision ID: 0808389e656e
Revises: 9ff64e1c35d3
Create Date: 2026-06-13 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "0808389e656e"
down_revision = "9ff64e1c35d3"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Apply Add task instance anchor to deadline and task_id to deadline_alert."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("task_instance_id", sa.Uuid(), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("deadline_task_instance_id_fkey"),
            "task_instance",
            ["task_instance_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_index("deadline_task_instance_id_idx", ["task_instance_id"], unique=False)

    with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
        batch_op.add_column(sa.Column("task_id", StringID(), nullable=True))
        batch_op.create_index(
            "deadline_alert_serialized_dag_id_task_id_idx",
            ["serialized_dag_id", "task_id"],
            unique=False,
        )


def downgrade():
    """Unapply Add task instance anchor to deadline and task_id to deadline_alert."""
    with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
        batch_op.drop_index("deadline_alert_serialized_dag_id_task_id_idx")
        batch_op.drop_column("task_id")

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_index("deadline_task_instance_id_idx")
        batch_op.drop_constraint(batch_op.f("deadline_task_instance_id_fkey"), type_="foreignkey")
        batch_op.drop_column("task_instance_id")
