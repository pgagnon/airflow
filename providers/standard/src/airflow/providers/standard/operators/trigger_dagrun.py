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

import datetime
import inspect
import json
from collections.abc import Mapping, Sequence
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, cast, overload

from airflow.providers.common.compat.sdk import (
    AirflowException,
    BaseOperatorLink,
    XCom,
    conf,
    timezone,
)
from airflow.providers.standard.utils.openlineage import safe_inject_openlineage_properties_into_dagrun_conf
from airflow.providers.standard.version_compat import (
    AIRFLOW_V_3_2_PLUS,
    BaseOperator,
    is_arg_set,
)
from airflow.sdk import DagRunType
from airflow.sdk.state import DagRunState

try:
    from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
except ImportError:
    from airflow.utils.types import NOTSET, ArgNotSet  # type: ignore[attr-defined,no-redef]

XCOM_LOGICAL_DATE_ISO = "trigger_logical_date_iso"
XCOM_RUN_ID = "trigger_run_id"


if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context, TaskInstanceKey


class DagIsPaused(AirflowException):
    """Raise when a dag is paused and something tries to run it."""

    def __init__(self, dag_id: str) -> None:
        super().__init__(dag_id)
        self.dag_id = dag_id

    def __str__(self) -> str:
        return f"Dag {self.dag_id} is paused"


class TriggerDagRunLink(BaseOperatorLink):
    """
    Operator link for TriggerDagRunOperator.

    It allows users to access DAG triggered by task using TriggerDagRunOperator.
    """

    name = "Triggered DAG"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        if TYPE_CHECKING:
            assert isinstance(operator, TriggerDagRunOperator)

        trigger_dag_id = operator.trigger_dag_id

        # Fetch the correct dag_run_id for the triggerED dag which is
        # stored in xcom during execution of the triggerING task.
        triggered_dag_run_id = XCom.get_value(ti_key=ti_key, key=XCOM_RUN_ID)

        from airflow.utils.helpers import build_airflow_dagrun_url

        return build_airflow_dagrun_url(dag_id=trigger_dag_id, run_id=triggered_dag_run_id)


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified DAG ID.

    Note that if database isolation mode is enabled, not all features are supported.

    :param trigger_dag_id: The ``dag_id`` of the DAG to trigger (templated).
    :param trigger_run_id: The run ID to use for the triggered DAG run (templated).
        If not provided, a run ID will be automatically generated.
    :param conf: Configuration for the DAG run (templated).
    :param logical_date: Logical date for the triggered DAG (templated).
    :param run_after: The date before which the triggered DAG should not run.
    :param reset_dag_run: Whether clear existing DAG run if already exists.
        This is useful when backfill or rerun an existing DAG run.
        This only resets (not recreates) the DAG run.
        DAG run conf is immutable and will not be reset on rerun of an existing DAG run.
        When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
        When reset_dag_run=True and dag run exists, existing DAG run will be cleared to rerun.
    :param wait_for_completion: Whether or not wait for DAG run completion. (default: False)
    :param poke_interval: Poke interval to check DAG run status when wait_for_completion=True.
        (default: 60)
    :param allowed_states: Optional list of allowed DAG run states of the triggered DAG. This is useful when
        setting ``wait_for_completion`` to True. Must be a valid DagRunState.
        Default is ``[DagRunState.SUCCESS]``.
    :param failed_states: Optional list of failed or disallowed DAG run states of the triggered DAG. This is
        useful when setting ``wait_for_completion`` to True. Must be a valid DagRunState.
        Default is ``[DagRunState.FAILED]``.
    :param skip_when_already_exists: Set to true to mark the task as SKIPPED if a DAG run of the triggered
        DAG for the same logical date already exists.
    :param fail_when_dag_is_paused: If the dag to trigger is paused, DagIsPaused will be raised. On
        Airflow 3.x this requires Airflow 3.2.0+ (it relies on the task-SDK DAG state endpoint added then);
        on Airflow 3.0/3.1 setting this raises ``NotImplementedError``.
    :param deferrable: If waiting for completion, whether to defer the task until done, default is ``False``.
    :param openlineage_inject_parent_info: whether to include OpenLineage metadata about the parent task
        in the triggered DAG run's conf, enabling improved lineage tracking. The metadata is only injected
        if OpenLineage is enabled and running. This option does not modify any other part of the conf,
        and existing OpenLineage-related settings in the conf will not be overwritten. The injection process
        is safeguarded against exceptions - if any error occurs during metadata injection, it is gracefully
        handled and the conf remains unchanged - so it's safe to use. Default is ``True``
    """

    template_fields: Sequence[str] = (
        "trigger_dag_id",
        "trigger_run_id",
        "logical_date",
        "conf",
        "wait_for_completion",
        "skip_when_already_exists",
    )

    template_fields_renderers = {"conf": "py"}
    ui_color = "#ffefeb"
    operator_extra_links = [TriggerDagRunLink()]

    def __init__(
        self,
        *,
        trigger_dag_id: str,
        trigger_run_id: str | None = None,
        conf: dict | None = None,
        logical_date: str | datetime.datetime | None | ArgNotSet = NOTSET,
        run_after: str | datetime.datetime | None | ArgNotSet = NOTSET,
        reset_dag_run: bool = False,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        allowed_states: list[str | DagRunState] | None = None,
        failed_states: list[str | DagRunState] | None = None,
        skip_when_already_exists: bool = False,
        fail_when_dag_is_paused: bool = False,
        note: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        openlineage_inject_parent_info: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.trigger_run_id = trigger_run_id
        self.conf = conf
        self.reset_dag_run = reset_dag_run
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        if allowed_states:
            self.allowed_states = [DagRunState(s) for s in allowed_states]
        else:
            self.allowed_states = [DagRunState.SUCCESS]
        if failed_states is not None:
            self.failed_states = [DagRunState(s) for s in failed_states]
        else:
            self.failed_states = [DagRunState.FAILED]
        self.skip_when_already_exists = skip_when_already_exists
        self.fail_when_dag_is_paused = fail_when_dag_is_paused
        self.openlineage_inject_parent_info = openlineage_inject_parent_info
        self.note = note
        self.deferrable = deferrable
        logical_date = _validate_datetime_param("logical_date", logical_date)
        run_after = _validate_datetime_param("run_after", run_after)
        self.logical_date = logical_date
        self.run_after = run_after
        if fail_when_dag_is_paused and not AIRFLOW_V_3_2_PLUS:
            raise NotImplementedError(
                "Setting `fail_when_dag_is_paused` requires Airflow 3.2.0+ on Airflow 3.x "
                "(it relies on the task-SDK DAG state endpoint added in 3.2.0)."
            )

    def execute(self, context: Context):
        if self.logical_date is NOTSET:
            if self.run_after is not NOTSET:
                parsed_logical_date = None
            else:
                # If no logical_date is provided we will set utcnow()
                parsed_logical_date = timezone.utcnow()
        else:
            logical_date = cast("str | datetime.datetime | None", self.logical_date)
            parsed_logical_date = _parse_datetime_param(logical_date)

        if self.run_after is NOTSET:
            parsed_run_after = parsed_logical_date
        else:
            run_after = cast("str | datetime.datetime | None", self.run_after)
            parsed_run_after = _parse_datetime_param(run_after)

        try:
            if self.conf and isinstance(self.conf, str):
                self.conf = json.loads(self.conf)
            json.dumps(self.conf)
        except (TypeError, JSONDecodeError):
            raise ValueError("conf parameter should be JSON Serializable %s", self.conf)

        if self.openlineage_inject_parent_info:
            self.log.debug("Checking if OpenLineage information can be safely injected into dagrun conf.")
            self.conf = safe_inject_openlineage_properties_into_dagrun_conf(
                dr_conf=self.conf, ti=context.get("ti")
            )

        if self.trigger_run_id:
            run_id = str(self.trigger_run_id)
        else:
            # Run-id generation currently lives on the core ORM model
            # (airflow.models.dagrun.DagRun.generate_run_id). The Task SDK does
            # not yet expose an equivalent run-id generator (airflow.sdk.DagRunType
            # has no generate_run_id), so this path needs airflow-core. Import it
            # lazily and guard with require_core so the module stays core-free and a
            # missing-core call errors with an actionable message.
            # NOTE (Phase-1 gap): a core-free run-id generator on airflow.sdk would
            # remove the last runtime core dependency of this operator.
            from airflow.sdk._core_compat import require_core

            with require_core("TriggerDagRunOperator auto run-id generation"):
                from airflow.models.dagrun import DagRun

            run_id = DagRun.generate_run_id(
                run_type=DagRunType.MANUAL,
                logical_date=parsed_logical_date,
                run_after=parsed_run_after or timezone.utcnow(),
            )

        # Save run_id as task attribute - to be used by listeners
        self.trigger_run_id = run_id

        if self.fail_when_dag_is_paused:
            # Tasks cannot access the ORM directly in Airflow 3.x; fetch the DAG state via the
            # task-SDK supervisor (GetDag execution-API endpoint, available from Airflow 3.2.0).
            if context["ti"].get_dag(self.trigger_dag_id).is_paused:
                raise DagIsPaused(dag_id=self.trigger_dag_id)

        self._trigger_dag_af_3(
            context=context,
            run_id=self.trigger_run_id,
            parsed_logical_date=parsed_logical_date,
            parsed_run_after=parsed_run_after if self.run_after is not NOTSET else None,
        )

    def _trigger_dag_af_3(self, context, run_id, parsed_logical_date, parsed_run_after=None):
        from airflow.providers.common.compat.sdk import DagRunTriggerException

        kwargs_accepted = dict(
            trigger_dag_id=self.trigger_dag_id,
            dag_run_id=run_id,
            conf=self.conf,
            logical_date=parsed_logical_date,
            reset_dag_run=self.reset_dag_run,
            skip_when_already_exists=self.skip_when_already_exists,
            wait_for_completion=self.wait_for_completion,
            allowed_states=self.allowed_states,
            failed_states=self.failed_states,
            poke_interval=self.poke_interval,
            deferrable=self.deferrable,
        )

        parameters = inspect.signature(DagRunTriggerException.__init__).parameters
        if self.note and "note" in parameters:
            kwargs_accepted["note"] = self.note

        if parsed_run_after and "run_after" in parameters:
            kwargs_accepted["run_after"] = parsed_run_after

        if isinstance(context, Mapping):
            from airflow.utils import helpers

            try:
                build_url_fn = getattr(helpers, "build_airflow_dagrun_url", None)
                ti = context.get("task_instance") or context.get("ti")

                if build_url_fn and ti and hasattr(ti, "xcom_push"):
                    ti.xcom_push(
                        key=TriggerDagRunLink().xcom_key,
                        value=build_url_fn(dag_id=self.trigger_dag_id, run_id=run_id),
                    )
            except (AttributeError, KeyError, TypeError, AssertionError) as e:
                self.log.debug(
                    "Skipping TriggerDagRunLink XCom push due to mock or incomplete context: %s", e
                )

        raise DagRunTriggerException(**kwargs_accepted)

    def execute_complete(self, context: Context, event: tuple[str, dict[str, Any]]):
        """
        Handle task completion after returning from a deferral.

        Args:
            context: The Airflow context dictionary.
            event: A tuple containing the class path of the trigger and the trigger event data.
        """
        # Example event tuple content:
        # (
        #  "airflow.providers.standard.triggers.external_task.DagStateTrigger",
        #  {
        #   'dag_id': 'some_dag',
        #   'states': ['success', 'failed'],
        #   'poll_interval': 15,
        #   'run_ids': ['manual__2025-11-19T17:49:20.907083+00:00'],
        #   'execution_dates': [
        #    DateTime(2025, 11, 19, 17, 49, 20, 907083, tzinfo=Timezone('UTC'))
        #   ]
        #  }
        # )
        _, event_data = event
        run_ids = event_data["run_ids"]
        # Re-set as attribute after coming back from deferral - to be used by listeners.
        # Just a safety check on length, we should always have single run_id here.
        self.trigger_run_id = run_ids[0] if len(run_ids) == 1 else None
        self._trigger_dag_run_af_3_execute_complete(event_data=event_data)

    def _trigger_dag_run_af_3_execute_complete(self, event_data: dict[str, Any]):
        failed_run_id_conditions = []

        for run_id in event_data["run_ids"]:
            state = event_data.get(run_id)
            if state in self.failed_states:
                failed_run_id_conditions.append(run_id)
                continue
            if state in self.allowed_states:
                self.log.info(
                    "%s finished with allowed state %s for run_id %s",
                    self.trigger_dag_id,
                    state,
                    run_id,
                )

        if failed_run_id_conditions:
            raise AirflowException(
                f"{self.trigger_dag_id} failed with failed states {self.failed_states} for run_ids"
                f" {failed_run_id_conditions}"
            )


@overload
def _validate_datetime_param(name: str, value: ArgNotSet) -> ArgNotSet: ...
@overload
def _validate_datetime_param(name: str, value: None) -> None: ...
@overload
def _validate_datetime_param(name: str, value: str) -> str: ...
@overload
def _validate_datetime_param(name: str, value: datetime.datetime) -> datetime.datetime: ...


def _validate_datetime_param(
    name: str,
    value: str | datetime.datetime | None | ArgNotSet,
) -> str | datetime.datetime | None | ArgNotSet:
    if not is_arg_set(value):
        return NOTSET
    if value is None or isinstance(value, (str, datetime.datetime)):
        return value
    raise TypeError(
        f"Expected str, datetime.datetime, or None for parameter '{name}'. Got {type(value).__name__}"
    )


@overload
def _parse_datetime_param(value: None) -> None: ...
@overload
def _parse_datetime_param(value: datetime.datetime) -> datetime.datetime: ...
@overload
def _parse_datetime_param(value: str) -> datetime.datetime: ...


def _parse_datetime_param(
    value: str | datetime.datetime | None,
) -> datetime.datetime | None:
    if value is None or isinstance(value, datetime.datetime):
        return value
    return timezone.parse(value)
