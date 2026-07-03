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

from collections.abc import Collection
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import func, select, tuple_

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select


def _get_count(
    dttm_filter,
    external_task_ids,
    external_task_group_id,
    external_dag_id,
    states,
    *,
    session: Session | None = None,
) -> int:
    """
    Get the count of records against dttm filter and states.

    This backs the deprecated public ``ExternalTaskSensor.get_count`` API, which
    queries the metadata DB directly. Direct ORM access requires airflow-core, so
    the core imports are loaded lazily here (guarded by ``require_core``) rather
    than at module import time - the rest of the provider stays core-free.

    :param dttm_filter: date time filter for logical date
    :param external_task_ids: The list of task_ids
    :param external_task_group_id: The ID of the external task group
    :param external_dag_id: The ID of the external DAG.
    :param states: task or dag states
    :param session: airflow session object
    """
    from airflow.sdk._core_compat import require_core

    with require_core("ExternalTaskSensor.get_count (direct metadata-DB access)"):
        from airflow.models import DagRun, TaskInstance
        from airflow.utils.session import provide_session

    @provide_session
    def _run(*, session: Session) -> int:
        return _get_count_impl(
            dttm_filter,
            external_task_ids,
            external_task_group_id,
            external_dag_id,
            states,
            DagRun,
            TaskInstance,
            session,
        )

    if session is not None:
        return _run(session=session)
    return _run()


def _get_count_impl(
    dttm_filter,
    external_task_ids,
    external_task_group_id,
    external_dag_id,
    states,
    DagRun,
    TaskInstance,
    session: Session,
) -> int:
    TI = TaskInstance
    DR = DagRun
    if not dttm_filter:
        return 0

    if external_task_ids:
        count = (
            session.scalar(
                _count_stmt(TI, states, dttm_filter, external_dag_id).where(TI.task_id.in_(external_task_ids))
            )
            or 0
        ) / len(external_task_ids)
    elif external_task_group_id:
        external_task_group_task_ids = _get_external_task_group_task_ids(
            dttm_filter, external_task_group_id, external_dag_id, session
        )
        if not external_task_group_task_ids:
            count = 0
        else:
            count = (
                (
                    session.scalar(
                        _count_stmt(TI, states, dttm_filter, external_dag_id).where(
                            tuple_(TI.task_id, TI.map_index).in_(external_task_group_task_ids)
                        )
                    )
                    or 0
                )
                / len(external_task_group_task_ids)
                * len(dttm_filter)
            )
    else:
        count = session.scalar(_count_stmt(DR, states, dttm_filter, external_dag_id)) or 0
    return cast("int", count)


def _count_stmt(
    model: type[DagRun] | type[TaskInstance], states: list[str], dttm_filter: list[Any], external_dag_id: str
) -> Select[tuple[int]]:
    """
    Get the count of records against dttm filter and states.

    :param model: The SQLAlchemy model representing the relevant table.
    :param states: task or dag states
    :param dttm_filter: date time filter for logical date
    :param external_dag_id: The ID of the external DAG.
    """
    return select(func.count()).where(
        model.dag_id == external_dag_id, model.state.in_(states), model.logical_date.in_(dttm_filter)
    )


def _get_external_task_group_task_ids(
    dttm_filter: list[Any], external_task_group_id: str, external_dag_id: str, session: Session
) -> list[tuple[str, int]]:
    """
    Get the count of records against dttm filter and states.

    This backs the deprecated public ``ExternalTaskSensor.get_external_task_group_task_ids``
    API. It reads the serialized DAG from the metadata DB, which requires
    airflow-core, so the ORM imports are loaded lazily (guarded by ``require_core``)
    instead of at module import time.

    :param dttm_filter: date time filter for logical date
    :param external_task_group_id: The ID of the external task group
    :param external_dag_id: The ID of the external DAG.
    :param session: airflow session object
    """
    from airflow.sdk._core_compat import require_core

    with require_core("ExternalTaskSensor.get_external_task_group_task_ids (serialized-DAG metadata access)"):
        from airflow.models import TaskInstance
        from airflow.models.serialized_dag import SerializedDagModel

    refreshed_dag_info = SerializedDagModel.get_dag(external_dag_id, session=session)
    if not refreshed_dag_info:
        return [(external_task_group_id, -1)]
    task_group = refreshed_dag_info.task_group_dict.get(external_task_group_id)

    if task_group:
        group_tasks = session.scalars(
            select(TaskInstance).filter(
                TaskInstance.dag_id == external_dag_id,
                TaskInstance.task_id.in_(task.task_id for task in task_group),
                TaskInstance.logical_date.in_(dttm_filter),
            )
        )

        return [(t.task_id, t.map_index) for t in group_tasks]

    # returning default task_id as group_id itself, this will avoid any failure in case of
    # 'check_existence=False' and will fail on timeout
    return [(external_task_group_id, -1)]


def _get_count_by_matched_states(
    run_id_task_state_map: dict[str, dict[str, Any]],
    states: Collection[str],
):
    count = 0
    for _, task_states in run_id_task_state_map.items():
        # Create this list such that len() can be checked in the conditional (to handle empty inner)
        matched_states: list = [state in states for state in task_states.values()]

        # An empty inner, such as {"r": {}}, results in count NOT being incremented
        if len(matched_states) > 0 and all(matched_states):
            count += 1

    return count
