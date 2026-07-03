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
AF2-EOL deletion guard for TriggerDagRunOperator.

These tests exercise the (now sole) Airflow-3 execute path without touching the
ORM/DB, so they run even when the local test DB schema is stale. They prove that
removing the Airflow-2 dead code left the Airflow-3 behaviour intact.
"""
from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import DagRunTriggerException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

TRIGGERED_DAG_ID = "triggerdag"


def test_execute_raises_dag_run_trigger_exception_with_generated_run_id():
    """The AF3 path raises DagRunTriggerException and generates a run_id (no DB)."""
    task = TriggerDagRunOperator(
        task_id="test_task",
        trigger_dag_id=TRIGGERED_DAG_ID,
        conf={"foo": "bar"},
    )

    with pytest.raises(DagRunTriggerException) as exc_info:
        task.execute(context={"ti": mock.MagicMock()})

    assert exc_info.value.trigger_dag_id == TRIGGERED_DAG_ID
    assert exc_info.value.conf == {"foo": "bar"}
    # run_id was generated and stored on the operator for listeners.
    assert task.trigger_run_id
    assert exc_info.value.dag_run_id == task.trigger_run_id


def test_af2_only_members_are_deleted():
    """The Airflow-2 dead code paths must no longer exist on the operator."""
    assert not hasattr(TriggerDagRunOperator, "_trigger_dag_af_2")
    assert not hasattr(TriggerDagRunOperator, "_trigger_dag_run_af_2_execute_complete")
    assert not hasattr(TriggerDagRunOperator, "attributes_not_supported_in_airflow_2")
