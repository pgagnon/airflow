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
"""SDK-only import isolation for the common.sql provider.

The provider must import on a Task-SDK-only install: importing its hooks,
operators, sensors, triggers and dialects must not drag any airflow-core module
into ``sys.modules``. We prove it in a fresh ``python -S`` interpreter whose
``sys.path`` deliberately excludes ``airflow-core/src`` so any accidental
``import airflow.<core>`` shows up as a leak (or fails outright).
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

# Walk up from tests/unit/common/sql/ to the worktree root.
_WORKTREE_ROOT = Path(__file__).resolve().parents[7]
_TASK_SDK_SRC = _WORKTREE_ROOT / "task-sdk" / "src"
_COMMON_SQL_SRC = _WORKTREE_ROOT / "providers" / "common" / "sql" / "src"
_COMMON_COMPAT_SRC = _WORKTREE_ROOT / "providers" / "common" / "compat" / "src"
_PROVIDER_SRC_ROOT = _COMMON_SQL_SRC / "airflow" / "providers" / "common" / "sql"

ALLOWED_PREFIXES = ("airflow.sdk", "airflow._shared", "airflow.sdk._shared", "airflow.providers")


def _sdk_only_sys_path() -> list[str]:
    """task-sdk + common.sql + common.compat src + site-packages + stdlib, NO airflow-core."""
    extra = [str(_TASK_SDK_SRC), str(_COMMON_SQL_SRC), str(_COMMON_COMPAT_SRC)]
    return extra + [p for p in sys.path if "airflow-core" not in p and p]


# Importing common.sql pulls `conf` from airflow.sdk.configuration. Touching
# `conf` triggers the SDK config init, which fans out into provider *discovery*
# (loading every installed provider's get_provider_info). That fan-out is
# orthogonal to whether common.sql itself imports airflow-core, and in this
# bare ``python -S`` path it also trips over sibling providers that are not yet
# migrated. We stub discovery to a no-op so the probe measures only what
# importing common.sql's own module chain pulls into sys.modules.
_IMPORT_PROBE = textwrap.dedent(
    """
    import sys

    import airflow.sdk.providers_manager_runtime as _pmr
    _pmr.discover_all_providers_from_packages = lambda *a, **k: None

    import airflow.providers.common.sql.hooks.sql
    import airflow.providers.common.sql.operators.sql
    import airflow.providers.common.sql.operators.generic_transfer
    import airflow.providers.common.sql.sensors.sql
    import airflow.providers.common.sql.triggers.sql
    import airflow.providers.common.sql.dialects.dialect

    allowed = ("airflow.sdk", "airflow._shared", "airflow.sdk._shared", "airflow.providers")

    def is_core(name):
        if not name.startswith("airflow.") or name == "airflow":
            return False
        for prefix in allowed:
            if name == prefix or name.startswith(prefix + "."):
                return False
        return True

    for name in sorted(n for n in sys.modules if is_core(n)):
        print(name)
    """
)


def test_common_sql_imports_without_airflow_core():
    """Importing common.sql's public modules pulls no airflow-core into sys.modules."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(_sdk_only_sys_path())
    result = subprocess.run(
        [sys.executable, "-S", "-c", _IMPORT_PROBE],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, f"importing common.sql modules failed:\n{result.stderr}"
    offenders = [line for line in result.stdout.splitlines() if line.strip()]
    assert offenders == [], (
        "importing common.sql modules leaked airflow-core modules into sys.modules:\n"
        + "\n".join(f"  - {name}" for name in offenders)
    )


def _is_core_module(name: str | None) -> bool:
    if not name or not name.startswith("airflow"):
        return False
    for prefix in ALLOWED_PREFIXES:
        if name == prefix or name.startswith(prefix + "."):
            return False
    return name == "airflow" or name.startswith("airflow.")


def _is_type_checking_guard(node: ast.stmt) -> bool:
    if not isinstance(node, ast.If):
        return False
    test = node.test
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"


def _top_level_core_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found: list[str] = []
    for node in tree.body:
        if _is_type_checking_guard(node):
            continue
        if isinstance(node, ast.ImportFrom):
            if _is_core_module(node.module):
                found.append(f"{path}:{node.lineno}: from {node.module} import ...")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _is_core_module(alias.name):
                    found.append(f"{path}:{node.lineno}: import {alias.name}")
    return found


def test_common_sql_source_has_no_top_level_core_imports():
    """No common.sql source module may import airflow-core at module scope."""
    assert _PROVIDER_SRC_ROOT.is_dir(), f"provider src root not found: {_PROVIDER_SRC_ROOT}"

    offenders: list[str] = []
    for path in sorted(_PROVIDER_SRC_ROOT.rglob("*.py")):
        offenders.extend(_top_level_core_imports(path))

    assert offenders == [], (
        "common.sql source must not import airflow-core at module scope. Move these into an "
        "`if TYPE_CHECKING:` block or into the function that needs core (lazy, via "
        "airflow.sdk._core_compat.require_core):\n" + "\n".join(f"  - {o}" for o in offenders)
    )
