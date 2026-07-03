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
"""SDK-only import isolation for the standard provider.

A migrated provider must be importable with only the Task SDK on the path -
no ``apache-airflow-core``. This probes the provider's hooks/operators/sensors/
triggers in a fresh ``python -S`` interpreter whose ``sys.path`` deliberately
excludes ``airflow-core/src`` and asserts that importing them pulls zero
airflow-core modules into ``sys.modules``.

Surviving airflow-core needs must live inside functions (lazy, routed through
``airflow.sdk._core_compat.require_core``) or inside ``if TYPE_CHECKING:`` blocks,
never at module scope.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

# providers/standard/tests/unit/standard/test_core_decoupling.py -> worktree root
_WORKTREE_ROOT = Path(__file__).resolve().parents[5]
_TASK_SDK_SRC = _WORKTREE_ROOT / "task-sdk" / "src"
_STANDARD_SRC = _WORKTREE_ROOT / "providers" / "standard" / "src"
_COMMON_COMPAT_SRC = _WORKTREE_ROOT / "providers" / "common" / "compat" / "src"
_COMMON_SQL_SRC = _WORKTREE_ROOT / "providers" / "common" / "sql" / "src"
_COMMON_IO_SRC = _WORKTREE_ROOT / "providers" / "common" / "io" / "src"
_SMTP_SRC = _WORKTREE_ROOT / "providers" / "smtp" / "src"


def _sdk_only_sys_path() -> list[str]:
    """SDK-only path: task-sdk + standard + sibling providers + site-packages, NO airflow-core.

    The sibling provider source dirs are present because importing ``airflow.sdk``
    config triggers provider discovery over installed entry points; the providers
    co-installed in this venv (common.io/sql/compat, smtp) must resolve to source
    so discovery doesn't fail. None of them is airflow-core, so they don't count
    as a core leak.
    """
    extra = [
        str(_TASK_SDK_SRC),
        str(_STANDARD_SRC),
        str(_COMMON_COMPAT_SRC),
        str(_COMMON_SQL_SRC),
        str(_COMMON_IO_SRC),
        str(_SMTP_SRC),
    ]
    # Keep site-packages + stdlib, drop any airflow source checkout (incl. airflow-core/src).
    kept = [p for p in sys.path if p and "airflow-core" not in p and "/src" not in p]
    return extra + kept


_PROBE = textwrap.dedent(
    """
    import sys

    # Accessing airflow.sdk config triggers full provider discovery over every
    # installed entry point. In this worktree some sibling providers are not yet
    # migrated (their __init__ still does ``from airflow import __version__``),
    # so discovery would explode on *their* code, not the standard provider's.
    # Neutralize the entry-point scan so this probe isolates the standard
    # provider's own import-time behavior. This does not hide any standard-provider
    # core import: those run when its modules are imported below regardless.
    import airflow.sdk.providers_manager_runtime as _pmr

    def _noop_discover(*args, **kwargs):
        return None

    _pmr.discover_all_providers_from_packages = _noop_discover

    import airflow.providers.standard
    import airflow.providers.standard.hooks.filesystem
    import airflow.providers.standard.hooks.subprocess
    import airflow.providers.standard.hooks.package_index
    import airflow.providers.standard.triggers.external_task
    import airflow.providers.standard.triggers.temporal
    import airflow.providers.standard.triggers.file
    import airflow.providers.standard.triggers.hitl
    import airflow.providers.standard.operators.python
    import airflow.providers.standard.operators.bash
    import airflow.providers.standard.operators.trigger_dagrun
    import airflow.providers.standard.operators.latest_only
    import airflow.providers.standard.operators.datetime
    import airflow.providers.standard.operators.weekday
    import airflow.providers.standard.operators.branch
    import airflow.providers.standard.operators.empty
    import airflow.providers.standard.operators.smooth
    import airflow.providers.standard.operators.hitl
    import airflow.providers.standard.sensors.external_task
    import airflow.providers.standard.sensors.date_time
    import airflow.providers.standard.sensors.filesystem
    import airflow.providers.standard.sensors.time
    import airflow.providers.standard.sensors.time_delta
    import airflow.providers.standard.sensors.python
    import airflow.providers.standard.sensors.bash
    import airflow.providers.standard.sensors.weekday
    import airflow.providers.standard.utils.skipmixin
    import airflow.providers.standard.utils.sensor_helper
    import airflow.providers.standard.utils.python_virtualenv
    import airflow.providers.standard.utils.weekday

    allowed = ("airflow.sdk", "airflow._shared", "airflow.sdk._shared", "airflow.providers")

    def is_core(name):
        if not name.startswith("airflow.") or name == "airflow":
            return False
        for prefix in allowed:
            if name == prefix or name.startswith(prefix + "."):
                return False
        return True

    for name in sorted(name for name in sys.modules if is_core(name)):
        print(name)
    """
)


def test_standard_provider_imports_without_airflow_core():
    """Importing the standard provider's public modules pulls no airflow-core."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(_sdk_only_sys_path())
    result = subprocess.run(
        [sys.executable, "-S", "-c", _PROBE],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, (
        f"importing standard provider modules failed under SDK-only path:\n{result.stderr}"
    )
    offenders = [line for line in result.stdout.splitlines() if line.strip()]
    assert offenders == [], (
        "importing the standard provider leaked airflow-core modules into sys.modules "
        "(these must become guarded lazy imports via require_core, or move under "
        "TYPE_CHECKING):\n" + "\n".join(f"  - {name}" for name in offenders)
    )
