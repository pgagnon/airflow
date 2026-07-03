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
"""SDK-only import isolation for the http provider.

The http provider must import using only the Task SDK (``airflow.sdk``) plus
sibling providers (``airflow.providers``), never airflow-core. We prove it in a
fresh ``python -S`` interpreter whose ``sys.path`` deliberately excludes
``airflow-core/src`` so any accidental ``import airflow.<core>`` either fails
outright or shows up as a leak in ``sys.modules``.

Two environmental concessions, neither of which can mask an airflow-core leak:

* The http provider is not pip-installed in the worktree, so a couple of its
  pure third-party runtime deps (``aiohttp``, ``requests_toolbelt``) may be
  absent. They are stubbed with concrete classes good enough for the provider
  modules to import; they are not ``airflow.*`` so they never affect the leak
  count.
* Provider entry-point discovery (triggered lazily the first time the SDK config
  is read) is disabled. Otherwise the SDK would try to import every installed
  provider's ``get_provider_info`` (standard, smtp, common.*), whose sources are
  not on this minimal path, and crash unrelated to the http provider.
"""

from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
import textwrap
from pathlib import Path

import pytest

# providers/http/tests/unit/http/test_core_decoupling.py -> worktree root
_WORKTREE_ROOT = Path(__file__).resolve().parents[5]
_TASK_SDK_SRC = _WORKTREE_ROOT / "task-sdk" / "src"
_HTTP_SRC = _WORKTREE_ROOT / "providers" / "http" / "src"
_COMMON_COMPAT_SRC = _WORKTREE_ROOT / "providers" / "common" / "compat" / "src"


def _sdk_only_sys_path() -> list[str]:
    """SDK-only child path: task-sdk + http + common.compat + site-packages, NO airflow-core."""
    site = sysconfig.get_paths()["purelib"]
    provider_srcs = [str(_TASK_SDK_SRC), str(_HTTP_SRC), str(_COMMON_COMPAT_SRC), site]
    # Keep stdlib paths from the running interpreter but drop anything pointing at
    # airflow-core/src or other provider src trees that could mask a leak.
    extra = [p for p in sys.path if p and "airflow-core" not in p and "/providers/" not in p]
    return provider_srcs + extra


_PROBE = textwrap.dedent(
    """
    import sys
    import types
    import typing

    # Disable provider entry-point discovery: the SDK would otherwise import every
    # installed provider's get_provider_info on first config access, but their src
    # trees are off this minimal path. This is unrelated to airflow-core leakage.
    from airflow.sdk._shared.providers_discovery import providers_discovery as _pd
    _pd.entry_points_with_dist = lambda group: iter(())

    # Stub the absent pure third-party deps with concrete classes (not airflow.*),
    # so the provider modules import far enough to reveal any airflow-core import.
    aiohttp = types.ModuleType("aiohttp")

    class BasicAuth(typing.NamedTuple):
        login: str = ""
        password: str = ""
        encoding: str = "latin1"

    class ClientResponseError(Exception):
        status = 0
        message = ""

    class ClientSession:
        ...

    aiohttp.BasicAuth = BasicAuth
    aiohttp.ClientResponseError = ClientResponseError
    aiohttp.ClientSession = ClientSession
    sys.modules["aiohttp"] = aiohttp

    _crr = types.ModuleType("aiohttp.client_reqrep")

    class ClientResponse:
        ...

    _crr.ClientResponse = ClientResponse
    sys.modules["aiohttp.client_reqrep"] = _crr

    for _n in ("requests_toolbelt", "requests_toolbelt.adapters"):
        sys.modules.setdefault(_n, types.ModuleType(_n))

    _rtaso = types.ModuleType("requests_toolbelt.adapters.socket_options")

    class TCPKeepAliveAdapter:
        def __init__(self, *args, **kwargs):
            ...

    _rtaso.TCPKeepAliveAdapter = TCPKeepAliveAdapter
    sys.modules["requests_toolbelt.adapters.socket_options"] = _rtaso

    import airflow.providers.http.hooks.http
    import airflow.providers.http.operators.http
    import airflow.providers.http.sensors.http
    import airflow.providers.http.triggers.http
    import airflow.providers.http.notifications.http

    allowed = ("airflow.sdk", "airflow._shared", "airflow.providers")

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


@pytest.mark.skipif(
    not (_TASK_SDK_SRC.is_dir() and _COMMON_COMPAT_SRC.is_dir()),
    reason="task-sdk / common.compat source trees not present in this layout",
)
def test_http_provider_imports_without_airflow_core():
    """Importing the http provider modules pulls no airflow-core into sys.modules."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(_sdk_only_sys_path())
    result = subprocess.run(
        [sys.executable, "-S", "-c", _PROBE],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, f"http provider failed to import under SDK-only path:\n{result.stderr}"
    offenders = [line for line in result.stdout.splitlines() if line.strip()]
    assert offenders == [], (
        "importing the http provider leaked airflow-core modules into sys.modules:\n"
        + "\n".join(f"  - {name}" for name in offenders)
    )
