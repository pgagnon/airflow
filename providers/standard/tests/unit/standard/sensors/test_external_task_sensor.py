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
from __future__ import annotations

import itertools
from datetime import timedelta
from unittest import mock

import pytest

from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DAG
from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowSkipException,
    TaskDeferred,
)
from airflow.providers.standard.exceptions import (
    ExternalDagFailedError,
    ExternalTaskFailedError,
    ExternalTaskGroupFailedError,
    ExternalTaskNotFoundError,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.providers.standard.triggers.external_task import WorkflowTrigger
from airflow.timetables.base import DataInterval
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import OperatorSerialization
from tests_common.test_utils.dag import create_scheduler_dag, sync_dag_to_db, sync_dags_to_db
from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.version_compat import (
    AIRFLOW_V_3_0_PLUS,
    AIRFLOW_V_3_1_PLUS,
    AIRFLOW_V_3_2_PLUS,
    AIRFLOW_V_3_3_PLUS,
)


def _make_dagbag(dag_folder):
    """DagBag with examples disabled on Airflow <3.3.

    In 3.3+, ``include_examples`` was removed and example DAGs come from
    provider example bundles instead. On older versions the default is True,
    which loads example DAGs that can fail tests with their required Params.
    """
    if AIRFLOW_V_3_3_PLUS:
        return DagBag(dag_folder=dag_folder)
    return DagBag(dag_folder=dag_folder, include_examples=False)  # type: ignore[call-arg]


if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task as task_deco
    from airflow.utils.types import DagRunTriggeredByType
else:
    from airflow.decorators import task as task_deco  # type: ignore[attr-defined,no-redef]

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk.timezone import coerce_datetime, datetime
else:
    from airflow.utils.timezone import coerce_datetime, datetime  # type: ignore[attr-defined,no-redef]

if AIRFLOW_V_3_2_PLUS:
    from airflow.dag_processing.dagbag import DagBag
else:
    from airflow.models.dagbag import DagBag  # type: ignore[attr-defined, no-redef]


pytestmark = pytest.mark.db_test

TI = TaskInstance

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"
TEST_TASK_ID = "time_sensor_check"
TEST_TASK_ID_ALTERNATE = "time_sensor_check_alternate"
TEST_TASK_GROUP_ID = "time_sensor_group_id"
DEV_NULL = "/dev/null"
TASK_ID = "external_task_sensor_check"
EXTERNAL_DAG_ID = "child_dag"  # DAG the external task sensor is waiting on
EXTERNAL_TASK_ID = "child_task"  # Task the external task sensor is waiting on
EXTERNAL_ID_AND_IDS_PROVIDE_ERROR = "Only one of `external_task_id` or `external_task_ids` may be provided to ExternalTaskSensor; use external_task_id or external_task_ids or external_task_group_id."
EXTERNAL_IDS_AND_TASK_GROUP_ID_PROVIDE_ERROR = "Only one of `external_task_group_id` or `external_task_ids` may be provided to ExternalTaskSensor; use external_task_id or external_task_ids or external_task_group_id."


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Different test for AF 2")
@pytest.mark.usefixtures("testing_dag_bundle")
class TestExternalTaskSensorV3:
    def setup_method(self):
        # Create a mock for TaskInstance with get_ti_count method
        mock_ti = mock.MagicMock()
        mock_ti.get_ti_count = mock.MagicMock(return_value=0)  # Default return value

        self.context = {
            "execution_date": DEFAULT_DATE,
            "logical_date": DEFAULT_DATE,
            "ti": mock_ti,
            "task": mock.MagicMock(),
            "run_id": "test_run_id",
        }

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_success(self, dag_maker):
        """Test that the sensor succeeds when the external task succeeds."""
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_external_task_sensor_success",
                allowed_states=["success"],
            )

        # Mimic DB response to get_ti_count as 1
        self.context["ti"].get_ti_count.return_value = 1

        op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=["success"],
            task_ids=["test_external_task_sensor_success"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_failure(self, dag_maker):
        """Test that the sensor fails when the external task fails."""
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_external_task_sensor_failure",
                failed_states=[State.FAILED],
            )

        self.context["ti"].get_ti_count.return_value = 1

        with pytest.raises(ExternalTaskFailedError):
            op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=[State.FAILED],
            task_ids=["test_external_task_sensor_failure"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_soft_fail(self, dag_maker):
        """Test that the sensor skips when soft_fail is True and external task fails."""
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_external_task_sensor_soft_fail",
                failed_states=[State.FAILED],
                soft_fail=True,
            )

        self.context["ti"].get_ti_count.return_value = 1

        with pytest.raises(AirflowSkipException):
            op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=[State.FAILED],
            task_ids=["test_external_task_sensor_soft_fail"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_multiple_task_ids(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_ids=["task1", "task2"],
                allowed_states=["success"],
            )

        self.context["ti"].get_ti_count.return_value = 2
        op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=["success"],
            task_ids=["task1", "task2"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_skipped_states(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_external_task_sensor_skipped_states",
                skipped_states=[State.SKIPPED],
            )

        self.context["ti"].get_ti_count.return_value = 1
        with pytest.raises(AirflowSkipException):
            op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=[State.SKIPPED],
            task_ids=["test_external_task_sensor_skipped_states"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    def test_external_task_sensor_invalid_combination(self, dag_maker):
        """Test that the sensor raises an error with invalid parameter combinations."""
        with pytest.raises(
            ValueError,
            match=EXTERNAL_ID_AND_IDS_PROVIDE_ERROR,
        ):
            with dag_maker("test_external_task_sensor_invalid_combination"):
                ExternalTaskSensor(
                    task_id="test_external_task_sensor_check",
                    external_dag_id="test_dag",
                    external_task_id="test_task",
                    external_task_ids=["test_task"],
                )

    def test_external_task_sensor_invalid_state(self, dag_maker):
        with pytest.raises(
            ValueError,
            match="Valid values for `allowed_states`, `skipped_states` and `failed_states` when `external_task_id` or `external_task_ids` or `external_task_group_id` is not `None",
        ):
            with dag_maker("test_external_task_sensor_invalid_state"):
                ExternalTaskSensor(
                    task_id="test_external_task_sensor_check",
                    external_dag_id="test_dag",
                    external_task_id="test_task",
                    allowed_states=["invalid_state"],
                )

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_task_group(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_group_id="test_group",
                allowed_states=["success"],
            )

        self.context["ti"].get_task_states.return_value = {"run_id": {"test_group.task_id": State.SUCCESS}}
        op.execute(context=self.context)

        self.context["ti"].get_task_states.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            task_group_id="test_group",
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_execution_date_fn(self, dag_maker):
        def execution_date_fn(dt):
            return [dt + timedelta(hours=1)]

        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_task",
                execution_date_fn=execution_date_fn,
                allowed_states=["success"],
            )

        self.context["ti"].get_ti_count.return_value = 1
        op.execute(context=self.context)

        expected_date = DEFAULT_DATE + timedelta(hours=1)
        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[expected_date],
            states=["success"],
            task_ids=["test_task"],
        )
        assert op.external_dates_filter == expected_date.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_execution_delta(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_task",
                execution_delta=timedelta(hours=1),
                allowed_states=["success"],
            )

        self.context["ti"].get_ti_count.return_value = 1
        op.execute(context=self.context)

        expected_date = DEFAULT_DATE - timedelta(hours=1)
        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[expected_date],
            states=["success"],
            task_ids=["test_task"],
        )
        assert op.external_dates_filter == expected_date.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_duplicate_task_ids(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_ids=["task1", "task1"],
                allowed_states=["success"],
            )

        with pytest.raises(ValueError, match="Duplicate task_ids passed in external_task_ids parameter"):
            op.execute(context=self.context)

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_deferrable(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_task",
                deferrable=True,
                allowed_states=["success"],
            )

        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=self.context)

        assert isinstance(exc.value.trigger, WorkflowTrigger)
        assert exc.value.trigger.external_dag_id == "test_dag_parent"
        assert exc.value.trigger.external_task_ids == ["test_task"]
        assert exc.value.trigger.logical_dates == [DEFAULT_DATE]

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_only_dag_id(self, dag_maker):
        """Test that the sensor works correctly when only external_dag_id is provided."""
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                allowed_states=["success"],
            )

        self.context["ti"].get_dr_count = mock.MagicMock(return_value=1)

        op.execute(context=self.context)

        self.context["ti"].get_dr_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=["success"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_task_group_failed_states(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_group_id="test_group",
                failed_states=[State.FAILED],
            )

        self.context["ti"].get_task_states.return_value = {"run_id": {"test_group.task_id": State.FAILED}}

        with pytest.raises(ExternalTaskGroupFailedError):
            op.execute(context=self.context)

        self.context["ti"].get_task_states.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            task_group_id="test_group",
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    def test_get_logical_date(self):
        """For AF 3, we check for logical date or dag_run.run_after  in context."""

        context = {"logical_date": DEFAULT_DATE}
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="test_dag_parent",
            external_task_id="test_task",
        )
        assert op._get_logical_date(context) == DEFAULT_DATE

    def test_get_logical_date_with_dag_run_after(self):
        """For AF 3, we check for logical date or dag_run.run_after  in context."""
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="test_dag_parent",
            external_task_id="test_task",
        )
        mock_dag_run = mock.MagicMock()
        mock_dag_run.run_after = DEFAULT_DATE
        context = {"dag_run": mock_dag_run}
        assert op._get_logical_date(context) == DEFAULT_DATE

    def test_handle_execution_date_fn(self):
        def func(dt, context):
            assert context["logical_date"] == dt
            return dt + timedelta(0)

        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="test_dag_parent",
            external_task_id="test_task",
            execution_date_fn=func,
        )
        context = {"logical_date": DEFAULT_DATE}
        assert op._handle_execution_date_fn(context) == DEFAULT_DATE


class TestExternalTaskAsyncSensor:
    TASK_ID = "external_task_sensor_check"
    EXTERNAL_DAG_ID = "child_dag"  # DAG the external task sensor is waiting on
    EXTERNAL_TASK_ID = "child_task"  # Task the external task sensor is waiting on

    def test_defer_and_fire_task_state_trigger(self):
        """
        Asserts that a task is deferred and TaskStateTrigger will be fired
        when the ExternalTaskAsyncSensor is provided with all required arguments
        (i.e. including the external_task_id).
        """
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context=context)

        assert isinstance(exc.value.trigger, WorkflowTrigger), "Trigger is not a WorkflowTrigger"

    def test_defer_and_fire_failed_state_trigger(self):
        """Tests that an ExternalTaskNotFoundError is raised in case of error event"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(ExternalTaskNotFoundError):
            sensor.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_defer_and_fire_timeout_state_trigger(self):
        """Tests that an ExternalTaskNotFoundError is raised in case of timeout event"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(ExternalTaskNotFoundError):
            sensor.execute_complete(
                context=context,
                event={"status": "timeout", "message": "Dag was not started within 1 minute, assuming fail."},
            )

    def test_defer_execute_check_correct_logging(self):
        """Asserts that logging occurs as expected"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with mock.patch.object(sensor.log, "info") as mock_log_info:
            sensor.execute_complete(
                context=context,
                event={"status": "success"},
            )
        mock_log_info.assert_called_with("External tasks %s has executed successfully.", [EXTERNAL_TASK_ID])

    def test_defer_execute_check_failed_status(self):
        """Tests that the execute_complete method properly handles the 'failed' status from WorkflowTrigger"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(ExternalDagFailedError, match="External job has failed."):
            sensor.execute_complete(
                context=context,
                event={"status": "failed"},
            )

    def test_defer_execute_check_failed_status_soft_fail(self):
        """Tests that the execute_complete method properly handles the 'failed' status with soft_fail=True"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
            soft_fail=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(AirflowSkipException, match="External job has failed skipping."):
            sensor.execute_complete(
                context=context,
                event={"status": "failed"},
            )

    def test_defer_with_failed_states(self):
        """Tests that failed_states are properly passed to the WorkflowTrigger when the sensor is deferred"""
        failed_states = ["failed", "upstream_failed"]
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
            failed_states=failed_states,
        )

        context = {"execution_date": datetime(2025, 1, 1), "logical_date": datetime(2025, 1, 1)}
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context=context)

        trigger = exc.value.trigger
        assert isinstance(trigger, WorkflowTrigger), "Trigger is not a WorkflowTrigger"
        assert trigger.failed_states == failed_states, "failed_states not properly passed to WorkflowTrigger"

    def test_defer_execute_complete_re_sets_external_dates_filter_attr(self):
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )
        assert sensor.external_dates_filter is None

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        sensor.execute_complete(context=context, event={"status": "success"})

        assert sensor.external_dates_filter == DEFAULT_DATE.isoformat()


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Needs Flask app context fixture for AF 2")
@pytest.mark.parametrize(
    argnames=("external_dag_id", "external_task_id", "expected_external_dag_id", "expected_external_task_id"),
    argvalues=[
        ("dag_test", "task_test", "dag_test", "task_test"),
        ("dag_{{ ds }}", "task_{{ ds }}", f"dag_{DEFAULT_DATE.date()}", f"task_{DEFAULT_DATE.date()}"),
    ],
    ids=["not_templated", "templated"],
)
def test_external_task_sensor_extra_link(
    external_dag_id,
    external_task_id,
    expected_external_dag_id,
    expected_external_task_id,
    create_task_instance_of_operator,
):
    ti = create_task_instance_of_operator(
        ExternalTaskSensor,
        dag_id="external_task_sensor_extra_links_dag",
        logical_date=DEFAULT_DATE,
        task_id="external_task_sensor_extra_links_task",
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
    )
    task = ti.render_templates()

    assert task.external_dag_id == expected_external_dag_id
    assert task.external_task_id == expected_external_task_id
    assert task.external_task_ids == [expected_external_task_id]

    url = task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key)

    assert f"/dags/{expected_external_dag_id}/runs" in url


class TestExternalTaskMarker:
    def test_serialized_fields(self):
        assert {"recursion_depth"}.issubset(ExternalTaskMarker.get_serialized_fields())

    def test_serialized_external_task_marker(self):
        dag = DAG("test_serialized_external_task_marker", schedule=None, start_date=DEFAULT_DATE)
        task = ExternalTaskMarker(
            task_id="parent_task",
            external_dag_id="external_task_marker_child",
            external_task_id="child_task1",
            dag=dag,
        )

        serialized_op = OperatorSerialization.serialize_operator(task)
        deserialized_op = OperatorSerialization.deserialize_operator(serialized_op)
        assert deserialized_op.task_type == "ExternalTaskMarker"
        assert getattr(deserialized_op, "external_dag_id") == "external_task_marker_child"
        assert getattr(deserialized_op, "external_task_id") == "child_task1"


@pytest.fixture
def dag_bag_ext():
    """
    Create a DagBag with DAGs looking like this. The dotted lines represent external dependencies
    set up using ExternalTaskMarker and ExternalTaskSensor.

    dag_0:   task_a_0 >> task_b_0
                             |
                             |
    dag_1:                   ---> task_a_1 >> task_b_1
                                                  |
                                                  |
    dag_2:                                        ---> task_a_2 >> task_b_2
                                                                       |
                                                                       |
    dag_3:                                                             ---> task_a_3 >> task_b_3
    """
    clear_db_runs()

    dag_bag = _make_dagbag(DEV_NULL)

    dag_0 = DAG("dag_0", start_date=DEFAULT_DATE, schedule=None)
    task_a_0 = EmptyOperator(task_id="task_a_0", dag=dag_0)
    task_b_0 = ExternalTaskMarker(
        task_id="task_b_0", external_dag_id="dag_1", external_task_id="task_a_1", recursion_depth=3, dag=dag_0
    )
    task_a_0 >> task_b_0

    dag_1 = DAG("dag_1", start_date=DEFAULT_DATE, schedule=None)
    task_a_1 = ExternalTaskSensor(
        task_id="task_a_1", external_dag_id=dag_0.dag_id, external_task_id=task_b_0.task_id, dag=dag_1
    )
    task_b_1 = ExternalTaskMarker(
        task_id="task_b_1", external_dag_id="dag_2", external_task_id="task_a_2", recursion_depth=2, dag=dag_1
    )
    task_a_1 >> task_b_1

    dag_2 = DAG("dag_2", start_date=DEFAULT_DATE, schedule=None)
    task_a_2 = ExternalTaskSensor(
        task_id="task_a_2", external_dag_id=dag_1.dag_id, external_task_id=task_b_1.task_id, dag=dag_2
    )
    task_b_2 = ExternalTaskMarker(
        task_id="task_b_2", external_dag_id="dag_3", external_task_id="task_a_3", recursion_depth=1, dag=dag_2
    )
    task_a_2 >> task_b_2

    dag_3 = DAG("dag_3", start_date=DEFAULT_DATE, schedule=None)
    task_a_3 = ExternalTaskSensor(
        task_id="task_a_3", external_dag_id=dag_2.dag_id, external_task_id=task_b_2.task_id, dag=dag_3
    )
    task_b_3 = EmptyOperator(task_id="task_b_3", dag=dag_3)
    task_a_3 >> task_b_3

    for dag in [dag_0, dag_1, dag_2, dag_3]:
        if AIRFLOW_V_3_0_PLUS:
            dag_bag.bag_dag(dag=dag)
        else:
            dag_bag.bag_dag(dag=dag, root_dag=dag)

    yield dag_bag

    clear_db_runs()


@pytest.fixture
def dag_bag_parent_child():
    """
    Create a DagBag with two DAGs looking like this. task_1 of child_dag_1 on day 1 depends on
    task_0 of parent_dag_0 on day 1. Therefore, when task_0 of parent_dag_0 on day 1 and day 2
    are cleared, parent_dag_0 DagRuns need to be set to running on both days, but child_dag_1
    only needs to be set to running on day 1.

                   day 1   day 2

     parent_dag_0  task_0  task_0
                     |
                     |
                     v
     child_dag_1   task_1  task_1

    """
    clear_db_runs()

    dag_bag = _make_dagbag(DEV_NULL)

    day_1 = DEFAULT_DATE

    with DAG("parent_dag_0", start_date=day_1, schedule=None) as dag_0:
        task_0 = ExternalTaskMarker(
            task_id="task_0",
            external_dag_id="child_dag_1",
            external_task_id="task_1",
            logical_date=day_1.isoformat(),
            recursion_depth=3,
        )

    with DAG("child_dag_1", start_date=day_1, schedule=None) as dag_1:
        ExternalTaskSensor(
            task_id="task_1",
            external_dag_id=dag_0.dag_id,
            external_task_id=task_0.task_id,
            execution_date_fn=lambda logical_date: day_1 if logical_date == day_1 else [],
            mode="reschedule",
        )

    for dag in [dag_0, dag_1]:
        if AIRFLOW_V_3_0_PLUS:
            dag_bag.bag_dag(dag=dag)
        else:
            dag_bag.bag_dag(dag=dag, root_dag=dag)

    yield dag_bag

    clear_db_runs()


@provide_session
def run_tasks(
    dag_bag: DagBag,
    *,
    logical_date=DEFAULT_DATE,
    session=NEW_SESSION,
) -> tuple[dict[str, DagRun], dict[str, TaskInstance]]:
    """
    Run all tasks in the DAGs in the given dag_bag. Return the TaskInstance objects as a dict
    keyed by task_id.
    """
    runs: dict[str, DagRun] = {}
    tis: dict[str, TaskInstance] = {}

    for dag in dag_bag.dags.values():
        data_interval = DataInterval(coerce_datetime(logical_date), coerce_datetime(logical_date))
        if AIRFLOW_V_3_0_PLUS:
            scheduler_dag = create_scheduler_dag(dag)
            runs[dag.dag_id] = dagrun = scheduler_dag.create_dagrun(
                run_id=scheduler_dag.timetable.generate_run_id(
                    run_type=DagRunType.MANUAL,
                    run_after=logical_date,
                    data_interval=data_interval,
                ),
                logical_date=logical_date,
                data_interval=data_interval,
                run_after=logical_date,
                run_type=DagRunType.MANUAL,
                triggered_by=DagRunTriggeredByType.TEST,
                state=DagRunState.RUNNING,
                start_date=logical_date,
                session=session,
            )
        else:
            runs[dag.dag_id] = dagrun = dag.create_dagrun(  # type: ignore[attr-defined,call-arg]
                run_id=dag.timetable.generate_run_id(  # type: ignore[attr-defined,call-arg,union-attr]
                    run_type=DagRunType.MANUAL,
                    logical_date=logical_date,
                    data_interval=data_interval,
                ),
                execution_date=logical_date,
                data_interval=data_interval,
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
                start_date=logical_date,
                session=session,
            )
        # we use sorting by task_id here because for the test DAG structure of ours
        # this is equivalent to topological sort. It would not work in general case
        # but it works for our case because we specifically constructed test DAGS
        # in the way that those two sort methods are equivalent
        tasks = sorted(dagrun.task_instances, key=lambda ti: ti.task_id)
        for ti in tasks:
            ti.refresh_from_task(dag.get_task(ti.task_id))
            tis[ti.task_id] = ti
            ti.run(session=session)
            session.flush()
            session.merge(ti)
            assert_ti_state_equal(ti, State.SUCCESS)

    return runs, tis


def assert_ti_state_equal(task_instance, state):
    """
    Assert state of task_instances equals the given state.
    """
    task_instance.refresh_from_db()
    assert task_instance.state == state


@provide_session
def clear_tasks(
    dag_bag,
    dag,
    task,
    *,
    start_date=DEFAULT_DATE,
    end_date=DEFAULT_DATE,
    dry_run=False,
    session=NEW_SESSION,
):
    """
    Clear the task and its downstream tasks recursively for the dag in the given dagbag.
    """
    partial: DAG = dag.partial_subset(task_ids_or_regex=[task.task_id], include_downstream=True)
    return partial.clear(
        start_date=start_date,
        end_date=end_date,
        dag_bag=dag_bag,
        dry_run=dry_run,
        session=session,
    )


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_transitive(dag_bag_ext):
    """
    Test clearing tasks across DAGs.
    """
    _, tis = run_tasks(dag_bag_ext)
    dag_0 = dag_bag_ext.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")
    clear_tasks(dag_bag_ext, dag_0, task_a_0)
    ti_a_0 = tis["task_a_0"]
    ti_b_3 = tis["task_b_3"]
    assert_ti_state_equal(ti_a_0, State.NONE)
    assert_ti_state_equal(ti_b_3, State.NONE)


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_clear_activate(dag_bag_parent_child):
    """
    Test clearing tasks across DAGs and make sure the right DagRuns are activated.
    """
    dag_bag = dag_bag_parent_child
    day_1 = DEFAULT_DATE
    day_2 = DEFAULT_DATE + timedelta(days=1)

    run_tasks(dag_bag, logical_date=day_1)
    run_tasks(dag_bag, logical_date=day_2)

    with create_session() as session:
        # Assert that dagruns of all the affected dags are set to SUCCESS before tasks are cleared.
        for dag, execution_date in itertools.product(dag_bag.dags.values(), [day_1, day_2]):
            dagrun = dag.get_dagrun(execution_date=execution_date, session=session)
            dagrun.set_state(State.SUCCESS)
        session.flush()

        dag_0 = dag_bag.get_dag("parent_dag_0")
        task_0 = dag_0.get_task("task_0")
        clear_tasks(dag_bag, dag_0, task_0, start_date=day_1, end_date=day_2, session=session)

        # Assert that dagruns of all the affected dags are set to QUEUED after tasks are cleared.
        # Unaffected dagruns should be left as SUCCESS.
        dagrun_0_1 = dag_bag.get_dag("parent_dag_0").get_dagrun(execution_date=day_1, session=session)
        dagrun_0_2 = dag_bag.get_dag("parent_dag_0").get_dagrun(execution_date=day_2, session=session)
        dagrun_1_1 = dag_bag.get_dag("child_dag_1").get_dagrun(execution_date=day_1, session=session)
        dagrun_1_2 = dag_bag.get_dag("child_dag_1").get_dagrun(execution_date=day_2, session=session)

    assert dagrun_0_1.state == State.QUEUED
    assert dagrun_0_2.state == State.QUEUED
    assert dagrun_1_1.state == State.QUEUED
    assert dagrun_1_2.state == State.SUCCESS


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_future(dag_bag_ext):
    """
    Test clearing tasks with no end_date. This is the case when users clear tasks with
    Future, Downstream and Recursive selected.
    """
    date_0 = DEFAULT_DATE
    date_1 = DEFAULT_DATE + timedelta(days=1)

    _, tis_date_0 = run_tasks(dag_bag_ext, logical_date=date_0)
    _, tis_date_1 = run_tasks(dag_bag_ext, logical_date=date_1)

    dag_0 = dag_bag_ext.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")
    # This should clear all tasks on dag_0 to dag_3 on both date_0 and date_1
    clear_tasks(dag_bag_ext, dag_0, task_a_0, end_date=None)

    ti_a_0_date_0 = tis_date_0["task_a_0"]
    ti_b_3_date_0 = tis_date_0["task_b_3"]
    ti_b_3_date_1 = tis_date_1["task_b_3"]
    assert_ti_state_equal(ti_a_0_date_0, State.NONE)
    assert_ti_state_equal(ti_b_3_date_0, State.NONE)
    assert_ti_state_equal(ti_b_3_date_1, State.NONE)


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_exception(dag_bag_ext):
    """
    Clearing across multiple DAGs should raise AirflowException if more levels are being cleared
    than allowed by the recursion_depth of the first ExternalTaskMarker being cleared.
    """
    run_tasks(dag_bag_ext)
    dag_0 = dag_bag_ext.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")
    task_b_0 = dag_0.get_task("task_b_0")
    task_b_0.recursion_depth = 2
    with pytest.raises(AirflowException, match="Maximum recursion depth 2"):
        clear_tasks(dag_bag_ext, dag_0, task_a_0)


@pytest.fixture
def dag_bag_cyclic():
    """
    Create a DagBag with DAGs having cyclic dependencies set up by ExternalTaskMarker and
    ExternalTaskSensor.

    dag_0:   task_a_0 >> task_b_0
                  ^          |
                  |          |
    dag_1:        |          ---> task_a_1 >> task_b_1
                  |                              ^
                  |                              |
    dag_n:        |                              ---> task_a_n >> task_b_n
                  |                                                   |
                  -----------------------------------------------------
    """

    def _factory(depth: int) -> DagBag:
        dag_bag = _make_dagbag(DEV_NULL)

        dags = []

        with DAG("dag_0", start_date=DEFAULT_DATE, schedule=None) as dag:
            dags.append(dag)
            task_a_0 = EmptyOperator(task_id="task_a_0")
            task_b_0 = ExternalTaskMarker(
                task_id="task_b_0", external_dag_id="dag_1", external_task_id="task_a_1", recursion_depth=3
            )
            task_a_0 >> task_b_0

        for n in range(1, depth):
            with DAG(f"dag_{n}", start_date=DEFAULT_DATE, schedule=None) as dag:
                dags.append(dag)
                task_a = ExternalTaskSensor(
                    task_id=f"task_a_{n}",
                    external_dag_id=f"dag_{n - 1}",
                    external_task_id=f"task_b_{n - 1}",
                )
                task_b = ExternalTaskMarker(
                    task_id=f"task_b_{n}",
                    external_dag_id=f"dag_{n + 1}",
                    external_task_id=f"task_a_{n + 1}",
                    recursion_depth=3,
                )
                task_a >> task_b

        # Create the last dag which loops back
        with DAG(f"dag_{depth}", start_date=DEFAULT_DATE, schedule=None) as dag:
            dags.append(dag)
            task_a = ExternalTaskSensor(
                task_id=f"task_a_{depth}",
                external_dag_id=f"dag_{depth - 1}",
                external_task_id=f"task_b_{depth - 1}",
            )
            task_b = ExternalTaskMarker(
                task_id=f"task_b_{depth}",
                external_dag_id="dag_0",
                external_task_id="task_a_0",
                recursion_depth=2,
            )
            task_a >> task_b

        for dag in dags:
            if AIRFLOW_V_3_0_PLUS:
                sync_dag_to_db(dag)
                dag_bag.bag_dag(dag=dag)
            else:
                dag_bag.bag_dag(dag=dag, root_dag=dag)  # type: ignore[call-arg]

        return dag_bag

    return _factory


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_cyclic_deep(dag_bag_cyclic):
    """
    Tests clearing across multiple DAGs that have cyclic dependencies. AirflowException should be
    raised.
    """
    dag_bag = dag_bag_cyclic(10)
    run_tasks(dag_bag)
    dag_0 = dag_bag.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")
    with pytest.raises(AirflowException, match="Maximum recursion depth 3"):
        clear_tasks(dag_bag, dag_0, task_a_0)


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_cyclic_shallow(dag_bag_cyclic):
    """
    Tests clearing across multiple DAGs that have cyclic dependencies shallower
    than recursion_depth
    """
    dag_bag = dag_bag_cyclic(2)
    run_tasks(dag_bag)
    dag_0 = dag_bag.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")

    tis = clear_tasks(dag_bag, dag_0, task_a_0, dry_run=True)

    assert sorted((ti.dag_id, ti.task_id) for ti in tis) == [
        ("dag_0", "task_a_0"),
        ("dag_0", "task_b_0"),
        ("dag_1", "task_a_1"),
        ("dag_1", "task_b_1"),
        ("dag_2", "task_a_2"),
        ("dag_2", "task_b_2"),
    ]


@pytest.fixture
def dag_bag_multiple(session):
    """
    Create a DagBag containing two DAGs, linked by multiple ExternalTaskMarker.
    """
    dag_bag = _make_dagbag(DEV_NULL)
    daily_dag = DAG("daily_dag", start_date=DEFAULT_DATE, schedule="@daily")
    agg_dag = DAG("agg_dag", start_date=DEFAULT_DATE, schedule="@daily")
    if AIRFLOW_V_3_0_PLUS:
        dag_bag.bag_dag(dag=daily_dag)
        dag_bag.bag_dag(dag=agg_dag)
    else:
        dag_bag.bag_dag(dag=daily_dag, root_dag=daily_dag)
        dag_bag.bag_dag(dag=agg_dag, root_dag=agg_dag)

    daily_task = EmptyOperator(task_id="daily_tas", dag=daily_dag)

    begin = EmptyOperator(task_id="begin", dag=agg_dag)
    for i in range(8):
        task = ExternalTaskMarker(
            task_id=f"{daily_task.task_id}_{i}",
            external_dag_id=daily_dag.dag_id,
            external_task_id=daily_task.task_id,
            logical_date=f"{{{{ macros.ds_add(ds, -1 * {i}) }}}}",
            dag=agg_dag,
        )
        begin >> task

    if AIRFLOW_V_3_0_PLUS:
        sync_dags_to_db([agg_dag, daily_dag])

    return dag_bag


@pytest.fixture
def dag_bag_head_tail(session):
    """
    Create a DagBag containing one DAG, with task "head" depending on task "tail" of the
    previous logical_date.

    20200501     20200502                 20200510
    +------+     +------+                 +------+
    | head |    -->head |    -->         -->head |
    |  |   |   / |  |   |   /           / |  |   |
    |  v   |  /  |  v   |  /           /  |  v   |
    | body | /   | body | /     ...   /   | body |
    |  |   |/    |  |   |/           /    |  |   |
    |  v   /     |  v   /           /     |  v   |
    | tail/|     | tail/|          /      | tail |
    +------+     +------+                 +------+
    """
    dag_bag = _make_dagbag(DEV_NULL)

    with DAG("head_tail", start_date=DEFAULT_DATE, schedule="@daily") as dag:
        head = ExternalTaskSensor(
            task_id="head",
            external_dag_id=dag.dag_id,
            external_task_id="tail",
            execution_delta=timedelta(days=1),
            mode="reschedule",
        )
        body = EmptyOperator(task_id="body")
        tail = ExternalTaskMarker(
            task_id="tail",
            external_dag_id=dag.dag_id,
            external_task_id=head.task_id,
            logical_date="{{ macros.ds_add(ds, 1) }}",
        )
        head >> body >> tail

    if AIRFLOW_V_3_0_PLUS:
        dag_bag.bag_dag(dag)
        sync_dag_to_db(dag)
    else:
        dag_bag.bag_dag(dag=dag, root_dag=dag)

    return dag_bag


@pytest.fixture
def dag_bag_head_tail_mapped_tasks(session):
    """
    Create a DagBag containing one DAG, with task "head" depending on task "tail" of the
    previous logical_date.

    20200501     20200502                 20200510
    +------+     +------+                 +------+
    | head |    -->head |    -->         -->head |
    |  |   |   / |  |   |   /           / |  |   |
    |  v   |  /  |  v   |  /           /  |  v   |
    | body | /   | body | /     ...   /   | body |
    |  |   |/    |  |   |/           /    |  |   |
    |  v   /     |  v   /           /     |  v   |
    | tail/|     | tail/|          /      | tail |
    +------+     +------+                 +------+
    """
    dag_bag = _make_dagbag(DEV_NULL)

    with DAG("head_tail", start_date=DEFAULT_DATE, schedule="@daily") as dag:

        @task_deco
        def dummy_task(x: int):
            return x

        head = ExternalTaskSensor(
            task_id="head",
            external_dag_id=dag.dag_id,
            external_task_id="tail",
            execution_delta=timedelta(days=1),
            mode="reschedule",
        )

        body = dummy_task.expand(x=range(5))
        tail = ExternalTaskMarker(
            task_id="tail",
            external_dag_id=dag.dag_id,
            external_task_id=head.task_id,
            logical_date="{{ macros.ds_add(ds, 1) }}",
        )
        head >> body >> tail

    if AIRFLOW_V_3_0_PLUS:
        sync_dag_to_db(dag)
    else:
        dag_bag.bag_dag(dag=dag, root_dag=dag)

    return dag_bag
