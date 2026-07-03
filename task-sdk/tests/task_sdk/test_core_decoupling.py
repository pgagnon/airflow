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
"""Import-isolation checks for decoupling the Task SDK from airflow-core.

Two complementary checks:

* ``test_sdk_source_has_no_top_level_airflow_core_imports`` - an AST invariant
  over the SDK source tree. No module under ``airflow/sdk`` may import
  airflow-core at module scope. This is the part the Task SDK owns and the gate
  that runs everywhere (core installed or not).
* ``test_authoring_dag_does_not_import_airflow_core`` - a runtime probe in a
  fresh interpreter asserting that ``import airflow.sdk`` + authoring a DAG pulls
  zero airflow-core modules into ``sys.modules``. This is only meaningful in an
  SDK-only install: when core is installed the shared ``airflow`` namespace
  forces core's ``airflow/__init__.py`` to run on any ``import airflow.*``, so it
  is skipped in that case (see the skipif reason).

Surviving airflow-core imports in the SDK belong inside ``if TYPE_CHECKING:``
(annotation-only) or inside the function that needs core, lazily, routed through
``airflow.sdk._core_compat.require_core``.
"""

from __future__ import annotations

import ast
import importlib.util
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

# The runtime probe below can only reach zero leaked core modules when
# ``apache-airflow-core`` is NOT installed: the ``airflow`` namespace is shared,
# so when core is present its ``airflow/__init__.py`` runs (eagerly importing
# ``airflow.configuration`` / ``airflow.settings``) before any ``airflow.sdk``
# code, pulling ~75 core modules in regardless of how clean the SDK source is.
# In a core-installed environment we therefore skip the runtime probe and rely on
# the AST source invariant (``test_sdk_source_has_no_top_level_airflow_core_imports``)
# which is the part the Task SDK source tree actually owns.
_CORE_INSTALLED = importlib.util.find_spec("airflow.configuration") is not None

# Module prefixes that legitimately belong to the SDK / shared-vendored code and
# are therefore NOT counted as airflow-core leakage.
ALLOWED_PREFIXES = ("airflow.sdk", "airflow._shared", "airflow.sdk._shared")

# The number of airflow-core modules currently pulled into sys.modules by
# `import airflow.sdk` + authoring a trivial DAG. This is the RED baseline.
# It should only ever go DOWN as decoupling phases land. When it reaches 0 the
# test passes. If it goes UP, a regression added a new top-level core import.
BASELINE_CORE_LEAK = 109

# Child program: author a DAG, then report leaked core modules as one-per-line.
_PROBE = textwrap.dedent(
    """
    import sys

    from airflow.sdk import DAG, task

    @task
    def my_task():
        return 1

    with DAG("import_isolation_probe"):
        my_task()

    allowed = ("airflow.sdk", "airflow._shared", "airflow.sdk._shared")

    def is_core(name):
        if not name.startswith("airflow."):
            return False
        for prefix in allowed:
            if name == prefix or name.startswith(prefix + "."):
                return False
        return True

    offenders = sorted(name for name in sys.modules if is_core(name))
    for name in offenders:
        print(name)
    """
)


def _collect_leaked_core_modules() -> list[str]:
    """Run the probe in a FRESH interpreter and return the leaked core modules."""
    result = subprocess.run(
        [sys.executable, "-c", _PROBE],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


@pytest.mark.skipif(
    _CORE_INSTALLED,
    reason=(
        "apache-airflow-core is installed; the shared `airflow` namespace forces "
        "core's airflow/__init__.py to run on any `import airflow.*`, so the runtime "
        "sys.modules probe can never reach zero here. The source-level invariant "
        "(test_sdk_source_has_no_top_level_airflow_core_imports) is the gate in this "
        "environment. This probe is the meaningful check in an SDK-only install."
    ),
)
def test_authoring_dag_does_not_import_airflow_core():
    """Authoring a DAG via the Task SDK must not import any airflow-core module.

    Only meaningful in an SDK-only install (core absent). Skipped when core is
    installed - see the module docstring and the skipif reason above.
    """
    offenders = _collect_leaked_core_modules()

    assert offenders == [], (
        f"`import airflow.sdk` + authoring a DAG leaked {len(offenders)} "
        f"airflow-core module(s) into sys.modules (baseline was "
        f"{BASELINE_CORE_LEAK}). These must become guarded lazy imports via "
        f"airflow.sdk._core_compat.require_core.\nLeaked modules:\n"
        + "\n".join(f"  - {name}" for name in offenders)
    )


# --- Source-level invariant -------------------------------------------------
#
# The goal-post test above measures the *runtime* effect of `import airflow.sdk`.
# Its residual offenders today come entirely from outside the Task SDK source
# tree: airflow-core's own ``airflow/__init__.py`` (which the namespace package
# forces to run) and the standard provider's ``@task`` python decorator chain
# (resolved lazily when ``task.python`` is accessed to author a DAG). Neither is
# fixable by editing Task SDK source.
#
# What the Task SDK *can* own is its own source tree: no module under
# ``airflow/sdk`` may perform an airflow-core import at import time. The test
# below enforces that invariant directly against the source via AST, so a future
# edit that reintroduces a top-level ``from airflow.<core> import ...`` is caught
# regardless of whether the runtime probe already trips on the core namespace.

_SDK_SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src" / "airflow" / "sdk"


def _is_core_module(name: str | None) -> bool:
    """True for an ``airflow.*`` module that is airflow-core (not SDK / shared)."""
    if not name or not name.startswith("airflow"):
        return False
    for prefix in ALLOWED_PREFIXES:
        if name == prefix or name.startswith(prefix + "."):
            return False
    return name == "airflow" or name.startswith("airflow.")


def _is_type_checking_guard(node: ast.stmt) -> bool:
    """True for ``if TYPE_CHECKING:`` / ``if typing.TYPE_CHECKING:`` blocks."""
    if not isinstance(node, ast.If):
        return False
    test = node.test
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"


def _top_level_core_imports(path: Path) -> list[str]:
    """Return import-time-executed airflow-core imports in a source file.

    Only module-level statements are inspected. Imports inside functions/methods
    (lazy imports) and inside ``if TYPE_CHECKING:`` blocks (annotation-only) are
    intentionally allowed and skipped.
    """
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


def test_sdk_source_has_no_top_level_airflow_core_imports():
    """No Task SDK source module may import airflow-core at import time.

    This is the invariant the Task SDK owns directly (its own source tree).
    airflow-core imports are fine inside ``if TYPE_CHECKING:`` (annotations) or
    inside functions (lazy, routed through ``require_core`` when the path truly
    needs core), but never at module scope where they run on import.
    """
    assert _SDK_SOURCE_ROOT.is_dir(), f"SDK source root not found: {_SDK_SOURCE_ROOT}"

    offenders: list[str] = []
    for path in sorted(_SDK_SOURCE_ROOT.rglob("*.py")):
        offenders.extend(_top_level_core_imports(path))

    assert offenders == [], (
        "Task SDK source must not import airflow-core at module scope. Move these "
        "into an `if TYPE_CHECKING:` block (annotation-only) or into the function "
        "that needs core (lazy import via airflow.sdk._core_compat.require_core):\n"
        + "\n".join(f"  - {o}" for o in offenders)
    )


# --- Provider-info schema availability (SDK-only installs) ------------------
#
# Provider discovery validates each provider's runtime info against JSON schemas
# that historically lived only in airflow-core (``airflow/provider_info.schema.json``
# and ``airflow/customized_form_field_behaviours.schema.json``). The shared
# ``providers_discovery`` reader looks them up via ``resource_files("airflow")``
# (resolves inside airflow-core) and falls back to ``Path(__file__).parent`` -
# i.e. next to the shared module. For a core-less SDK install only the fallback is
# reachable, so the schemas must ship inside the SDK's bundled shared package. They
# are vendored alongside ``providers_discovery`` in ``shared/`` so both the
# task-sdk and airflow-core wheels carry them via their existing force-include.


@pytest.mark.parametrize(
    "schema_name",
    ["provider_info.schema.json", "customized_form_field_behaviours.schema.json"],
)
def test_provider_info_schema_bundled_with_sdk(schema_name: str):
    """The provider-info JSON schemas must sit next to the SDK's shared discovery module.

    This is what makes provider discovery work in a core-less SDK install: the
    reader's local-file fallback (``Path(__file__).parent / filename``) resolves
    here without airflow-core on the path.
    """
    from airflow.sdk._shared.providers_discovery import providers_discovery

    schema_path = Path(providers_discovery.__file__).parent / schema_name
    assert schema_path.is_file(), (
        f"{schema_name} must be bundled next to the SDK's providers_discovery module "
        f"({schema_path}) so provider discovery works without apache-airflow-core."
    )

    import json

    # Must be a real, parseable JSON schema (guards against an empty/broken copy).
    schema = json.loads(schema_path.read_text())
    assert isinstance(schema, dict) and schema, f"{schema_name} is not a valid JSON schema object"


def test_provider_info_schema_validator_builds_without_resource_lookup(monkeypatch):
    """The schema validator builds even when ``resource_files('airflow')`` can't find the file.

    Simulates the core-less case (no ``airflow/provider_info.schema.json`` at the
    ``airflow`` package root) by forcing the resource lookup to miss, proving the
    local-file fallback - backed by the bundled schema - carries the load.
    """
    from airflow.sdk._shared.providers_discovery import providers_discovery

    def _missing_resource(_package):
        raise FileNotFoundError("simulated: airflow-core not installed")

    monkeypatch.setattr(providers_discovery, "resource_files", _missing_resource)

    # Should fall back to the bundled schema and build a working validator.
    validator = providers_discovery._create_provider_info_schema_validator()
    assert validator is not None


# --- Triggers base module: core-free import ---------------------------------
#
# The canonical ``triggers.base`` module lives in the Task SDK at
# ``airflow.sdk.triggers.base`` (``airflow.triggers.base`` re-exports it for
# backwards compatibility). Importing it via the SDK path must not drag any
# airflow-core module into ``sys.modules`` - that is the whole point of the
# relocation. We prove it in a fresh ``python -S`` interpreter whose ``sys.path``
# deliberately excludes ``airflow-core/src`` so that any accidental
# ``import airflow.<core>`` would either fail or show up as a leak.

_WORKTREE_ROOT = Path(__file__).resolve().parents[3]
_TASK_SDK_SRC = _WORKTREE_ROOT / "task-sdk" / "src"


def _sdk_only_sys_path() -> list[str]:
    """sys.path for an SDK-only child: task-sdk src + site-packages + stdlib, NO airflow-core."""
    return [str(_TASK_SDK_SRC)] + [p for p in sys.path if "airflow-core" not in p and p]


_SHARED_TRIGGERS_PROBE = textwrap.dedent(
    """
    import sys

    import airflow.sdk.triggers.base as base

    # Sanity: the public trigger surface resolved from the shared module.
    assert base.BaseTrigger
    assert base.BaseEventTrigger
    assert base.TriggerEvent
    assert base.StartTriggerArgs
    assert base.TaskSuccessEvent

    allowed = ("airflow.sdk", "airflow._shared", "airflow.sdk._shared")

    def is_core(name):
        if not name.startswith("airflow."):
            return False
        if name == "airflow":
            return False
        for prefix in allowed:
            if name == prefix or name.startswith(prefix + "."):
                return False
        return True

    offenders = sorted(name for name in sys.modules if is_core(name))
    for name in offenders:
        print(name)
    """
)


def test_shared_triggers_base_imports_without_airflow_core():
    """Importing the SDK triggers base module pulls no airflow-core."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(_sdk_only_sys_path())
    result = subprocess.run(
        [sys.executable, "-S", "-c", _SHARED_TRIGGERS_PROBE],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, (
        f"probe failed to import airflow.sdk.triggers.base:\n{result.stderr}"
    )
    offenders = [line for line in result.stdout.splitlines() if line.strip()]
    assert offenders == [], (
        "importing airflow.sdk.triggers.base leaked airflow-core modules:\n"
        + "\n".join(f"  - {name}" for name in offenders)
    )


def test_legacy_airflow_triggers_base_still_imports():
    """The historical ``airflow.triggers.base`` path keeps re-exporting the trigger API.

    153 providers and 15 airflow-core modules import from this path; the relocation
    into the shared library must not break a single one of them.
    """
    # The legacy names must be the very same objects the SDK module defines,
    # so isinstance()/issubclass() across the two import paths stay consistent.
    from airflow.sdk.triggers import base as shared_base
    from airflow.triggers.base import (
        BaseEventTrigger,
        BaseTaskEndEvent,
        BaseTrigger,
        StartTriggerArgs,
        TaskFailedEvent,
        TaskSkippedEvent,
        TaskSuccessEvent,
        TriggerEvent,
    )

    assert BaseTrigger is shared_base.BaseTrigger
    assert BaseEventTrigger is shared_base.BaseEventTrigger
    assert TriggerEvent is shared_base.TriggerEvent
    assert StartTriggerArgs is shared_base.StartTriggerArgs
    assert TaskSuccessEvent is shared_base.TaskSuccessEvent
    assert TaskFailedEvent is shared_base.TaskFailedEvent
    assert TaskSkippedEvent is shared_base.TaskSkippedEvent
    assert BaseTaskEndEvent is shared_base.BaseTaskEndEvent

    # And a deferrable-operator style round trip still works end to end.
    assert issubclass(BaseEventTrigger, BaseTrigger)
    assert TaskSuccessEvent().task_instance_state == "success"


_SDK_TRIGGERS_PUBLIC_PROBE = textwrap.dedent(
    """
    import sys

    from airflow.sdk.triggers import (
        BaseEventTrigger,
        BaseTrigger,
        StartTriggerArgs,
        TaskSuccessEvent,
        TriggerEvent,
    )

    assert issubclass(BaseEventTrigger, BaseTrigger)
    assert StartTriggerArgs(trigger_cls="x", next_method="m").trigger_kwargs is None
    assert repr(TriggerEvent(1)) == "TriggerEvent<1>"
    assert TaskSuccessEvent().task_instance_state == "success"

    allowed = ("airflow.sdk", "airflow._shared", "airflow.sdk._shared")

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


def test_public_sdk_triggers_path_is_core_free():
    """``airflow.sdk.triggers`` exposes the trigger API for providers without airflow-core.

    This is the path Phase 2 migrates providers onto; it must resolve in an
    SDK-only install and leak no airflow-core modules.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(_sdk_only_sys_path())
    result = subprocess.run(
        [sys.executable, "-S", "-c", _SDK_TRIGGERS_PUBLIC_PROBE],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, f"importing airflow.sdk.triggers failed:\n{result.stderr}"
    offenders = [line for line in result.stdout.splitlines() if line.strip()]
    assert offenders == [], (
        "importing airflow.sdk.triggers leaked airflow-core modules:\n"
        + "\n".join(f"  - {name}" for name in offenders)
    )
