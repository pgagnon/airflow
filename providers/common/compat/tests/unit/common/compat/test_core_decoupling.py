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
"""SDK-only import isolation for the common.compat provider.

This provider is a compatibility shim. It must import using only the Task SDK
(``airflow.sdk``) and other providers - never airflow-core at module import
time. We prove it in a fresh ``python -S`` interpreter whose ``sys.path``
deliberately excludes ``airflow-core/src`` so any accidental ``import
airflow.<core>`` would either fail or show up as a leak in ``sys.modules``.

Genuinely core-only asset-compat symbols (``expand_alias_to_assets``,
``AssetDetails``, ``AssetAliasDetails``) are exposed lazily via module
``__getattr__`` so importing the module stays core-free; only *accessing* those
names pulls core.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

# tests/unit/common/compat/test_core_decoupling.py -> worktree root is parents[6]
_WORKTREE_ROOT = Path(__file__).resolve().parents[6]
_TASK_SDK_SRC = _WORKTREE_ROOT / "task-sdk" / "src"
_COMPAT_SRC = _WORKTREE_ROOT / "providers" / "common" / "compat" / "src"


def _sdk_only_sys_path() -> list[str]:
    """sys.path for an SDK-only child: task-sdk + provider src + site-packages + stdlib, NO core."""
    return (
        [str(_TASK_SDK_SRC), str(_COMPAT_SRC)]
        + [p for p in sys.path if "airflow-core" not in p and p]
    )


# Import the provider's module surface that providers consume at import time.
# Each must resolve without dragging airflow-core into sys.modules.
#
# Importing ``airflow.sdk.definitions.asset`` eagerly initializes the SDK config,
# which triggers provider auto-discovery (it walks every installed
# ``apache_airflow_provider`` entry point and imports the package). In this shared
# venv sibling providers are not yet SDK-migrated, so loading them would crash the
# probe for reasons unrelated to common.compat. We pin discovery to this provider's
# own entry point so the leak check reflects *this* provider's import graph only.
_PROBE = textwrap.dedent(
    """
    import sys

    from airflow.sdk._shared.providers_discovery import providers_discovery as _pd

    _orig = _pd.entry_points_with_dist

    def _only_common_compat(group):
        for ep, dist in _orig(group):
            name = (dist.metadata["name"] if dist.metadata else "") or ""
            if name == "apache-airflow-providers-common-compat":
                yield ep, dist

    _pd.entry_points_with_dist = _only_common_compat

    import airflow.providers.common.compat.assets
    import airflow.providers.common.compat.module_loading
    import airflow.providers.common.compat.notifier
    import airflow.providers.common.compat.connection
    import airflow.providers.common.compat.sdk
    import airflow.providers.common.compat.version_compat
    import airflow.providers.common.compat.lineage.entities
    import airflow.providers.common.compat.lineage.hook
    import airflow.providers.common.compat.check
    import airflow.providers.common.compat.security.permissions
    import airflow.providers.common.compat.standard.operators
    import airflow.providers.common.compat.standard.triggers
    import airflow.providers.common.compat.standard.utils
    import airflow.providers.common.compat.openlineage.facet
    import airflow.providers.common.compat.openlineage.check

    # The SDK-backed asset names resolve without core.
    from airflow.providers.common.compat.assets import Asset, AssetAlias, AssetAll, AssetAny
    assert Asset and AssetAlias and AssetAll and AssetAny

    # module_loading must resolve to the SDK implementation.
    from airflow.providers.common.compat.module_loading import (
        import_string,
        is_valid_dotpath,
        iter_namespace,
        qualname,
    )
    assert callable(import_string)
    assert is_valid_dotpath("a.b.c") is True

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


def test_compat_provider_imports_without_airflow_core():
    """Importing the common.compat provider modules must pull no airflow-core."""
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
        f"probe failed to import common.compat provider modules:\n{result.stderr}"
    )
    offenders = [line for line in result.stdout.splitlines() if line.strip()]
    assert offenders == [], (
        "importing the common.compat provider leaked airflow-core modules:\n"
        + "\n".join(f"  - {name}" for name in offenders)
    )
