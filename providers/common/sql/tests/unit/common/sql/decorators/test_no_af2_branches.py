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
"""Guard tests proving the AF2-only version branches were removed from the SQL decorators.

AF2 reached end of life on 2026-04-22; the ``if AIRFLOW_V_3_0_PLUS:`` branches in the
decorator modules were dead code. These tests pin the AF3 behavior: ``SET_DURING_EXECUTION``
is imported directly from the SDK and no module-level ``AIRFLOW_V_3_0_PLUS`` guard remains.
"""

from __future__ import annotations

import importlib
import inspect

import pytest

from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION as SDK_SET_DURING_EXECUTION

DECORATOR_MODULES = [
    "airflow.providers.common.sql.decorators.sql",
    "airflow.providers.common.sql.decorators.analytics",
]


@pytest.mark.parametrize("module_path", DECORATOR_MODULES)
def test_set_during_execution_comes_from_sdk(module_path):
    """The sentinel resolves to the AF3 SDK object, not the AF2 ``airflow.utils.types.NOTSET``."""
    module = importlib.import_module(module_path)
    assert module.SET_DURING_EXECUTION is SDK_SET_DURING_EXECUTION


@pytest.mark.parametrize("module_path", DECORATOR_MODULES)
def test_no_af2_version_branch_in_source(module_path):
    """No ``AIRFLOW_V_3_0_PLUS`` guard or AF2 ``airflow.utils.types`` import survives."""
    module = importlib.import_module(module_path)
    source = inspect.getsource(module)
    assert "AIRFLOW_V_3_0_PLUS" not in source
    assert "airflow.utils.types" not in source
