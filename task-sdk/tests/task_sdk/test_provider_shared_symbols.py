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
"""Public SDK homes for small shared symbols providers reach into airflow-core for.

These verify the *behaviour* providers depend on: that the symbol is importable
from a stable public ``airflow.sdk`` path. Phase 2 repoints providers at these
paths so they no longer import airflow-core.
"""

from __future__ import annotations


class TestLoggingMixin:
    def test_importable_from_log_module(self):
        from airflow.sdk.log import LoggingMixin

        class Foo(LoggingMixin):
            pass

        # The mixin gives a class a configured logger named after the class.
        assert Foo().log is not None

    def test_importable_from_sdk_top_level(self):
        from airflow.sdk import LoggingMixin as TopLevel
        from airflow.sdk.log import LoggingMixin as FromLog

        assert TopLevel is FromLog


class TestMergeDicts:
    def test_merge_dicts_recursive_non_mutating(self):
        from airflow.sdk import merge_dicts

        a = {"x": {"y": 1}, "z": 2}
        b = {"x": {"w": 3}, "z": 9}
        merged = merge_dicts(a, b)

        assert merged == {"x": {"y": 1, "w": 3}, "z": 9}
        # input is not mutated
        assert a == {"x": {"y": 1}, "z": 2}


class TestKwargsHelpers:
    def test_determine_kwargs_filters_to_signature(self):
        from airflow.sdk.bases.decorator import determine_kwargs

        def fn(a, b):
            return a + b

        assert determine_kwargs(fn, (), {"a": 1, "b": 2, "c": 3}) == {"a": 1, "b": 2}

    def test_make_kwargs_callable_forwards_only_needed(self):
        from airflow.sdk.bases.decorator import make_kwargs_callable

        def fn(a, b):
            return a + b

        wrapped = make_kwargs_callable(fn)
        assert wrapped(a=1, b=2, c=99) == 3


class TestStateAndTypes:
    def test_dag_run_type_public(self):
        from airflow.sdk import DagRunType

        assert DagRunType.MANUAL.value == "manual"

    def test_dag_run_state_public(self):
        from airflow.sdk import DagRunState

        assert DagRunState.SUCCESS.value == "success"

    def test_task_instance_state_public(self):
        from airflow.sdk import TaskInstanceState

        assert TaskInstanceState.RUNNING.value == "running"

    def test_state_helper_class(self):
        from airflow.sdk.state import State, TaskInstanceState

        assert State.SUCCESS == TaskInstanceState.SUCCESS
        assert TaskInstanceState.SUCCESS in State.finished


class TestProviderExceptions:
    def test_provider_deprecation_warning(self):
        from airflow.sdk.exceptions import AirflowProviderDeprecationWarning

        assert issubclass(AirflowProviderDeprecationWarning, DeprecationWarning)

    def test_deserializing_result_error(self):
        from airflow.sdk.exceptions import DeserializingResultError

        assert issubclass(DeserializingResultError, ValueError)

    def test_config_exception(self):
        from airflow.sdk.exceptions import AirflowConfigException

        assert issubclass(AirflowConfigException, Exception)
