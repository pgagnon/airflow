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
"""SDK-only import-isolation check for the smtp provider.

Importing the provider's public surface (hooks/operators/notifications) must not
pull any ``apache-airflow-core`` module into ``sys.modules``. The provider may
depend only on the Task SDK (``airflow.sdk``) and on other providers
(``airflow.providers``).

The probe runs in a fresh ``python -S`` interpreter whose ``sys.path`` is
constructed to mirror a core-less Task-SDK-only install: the task-sdk src, the
required provider srcs, site-packages and the stdlib, but explicitly NOT
``airflow-core/src``. Any accidental ``import airflow.<core>`` therefore shows up
as a leaked module (or fails outright), which the assertion catches.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

# providers/smtp/tests/unit/smtp/test_core_decoupling.py -> worktree root
_WORKTREE_ROOT = Path(__file__).resolve().parents[5]
_TASK_SDK_SRC = _WORKTREE_ROOT / "task-sdk" / "src"
_PROVIDER_SRCS = [
    _WORKTREE_ROOT / "providers" / "smtp" / "src",
    _WORKTREE_ROOT / "providers" / "common" / "compat" / "src",
]


def _sdk_only_sys_path() -> list[str]:
    """sys.path for an SDK-only child: task-sdk + this provider's deps + site-packages + stdlib.

    Excludes ``airflow-core/src`` (the whole point) and every other provider's editable
    ``.../providers/<x>/src`` entry, keeping only the srcs smtp actually depends on
    (task-sdk and common.compat). That mirrors a real core-less install of the smtp
    provider and keeps peer providers out of the probe.
    """
    extra = [str(_TASK_SDK_SRC)] + [str(p) for p in _PROVIDER_SRCS]
    inherited = [
        p
        for p in sys.path
        if p
        and "airflow-core" not in p
        and "/providers/" not in p.replace(os.sep, "/")
    ]
    return extra + inherited


_PROBE = textwrap.dedent(
    """
    import sys

    # Neutralize SDK provider discovery. Resolving e.g. ``airflow.sdk.BaseOperator``
    # reads a config value at import time, which eagerly scans EVERY installed
    # provider's entry point (via dist metadata) and imports its top-level package.
    # That is SDK-internal behavior owned by the Task SDK, not by this provider, and
    # in a worktree where all providers are installed it would drag in unrelated
    # (possibly still core-coupled) peer providers. Stubbing it isolates *this*
    # provider's own import graph, which is what this test asserts on.
    import airflow.sdk.providers_manager_runtime as _pmr
    _pmr.ProvidersManagerTaskRuntime.initialize_providers_list = lambda self: None
    _pmr.ProvidersManagerTaskRuntime.initialize_provider_configs = lambda self: None

    import airflow.providers.smtp
    import airflow.providers.smtp.hooks.smtp
    import airflow.providers.smtp.operators.smtp
    import airflow.providers.smtp.notifications.smtp

    # Sanity: the public surface resolved.
    from airflow.providers.smtp.hooks.smtp import SmtpHook
    from airflow.providers.smtp.operators.smtp import EmailOperator
    from airflow.providers.smtp.notifications.smtp import SmtpNotifier

    assert SmtpHook and EmailOperator and SmtpNotifier

    allowed = ("airflow.sdk", "airflow.providers", "airflow._shared")

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


def test_smtp_provider_imports_without_airflow_core():
    """Importing the smtp provider's modules must not pull in airflow-core."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(_sdk_only_sys_path())
    # Drop variables that could re-add airflow-core via a sitecustomize/coverage hook.
    env.pop("COV_CORE_SOURCE", None)
    result = subprocess.run(
        [sys.executable, "-S", "-c", _PROBE],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, f"SDK-only import of the smtp provider failed:\n{result.stderr}"
    offenders = [line for line in result.stdout.splitlines() if line.strip()]
    assert offenders == [], (
        "importing the smtp provider leaked airflow-core modules into sys.modules:\n"
        + "\n".join(f"  - {name}" for name in offenders)
    )
