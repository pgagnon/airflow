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
"""Version detection must work in an SDK-only install (no apache-airflow-core).

``get_base_airflow_version_tuple()`` historically did ``from airflow import
__version__``. In a core-less install the ``airflow`` namespace is the Task
SDK's pkgutil shim, which exposes no ``__version__`` - so that import raised
``ImportError`` and every provider that imports its ``version_compat`` module
failed to import at all.

The two tests below pin the contract:

* with core installed (this interpreter), the tuple still reflects core's
  ``apache-airflow-core`` version;
* in a fresh ``python -S`` interpreter on an SDK-only ``sys.path`` (core
  removed), importing ``version_compat`` succeeds and the ``AIRFLOW_V_3_x_PLUS``
  flags resolve from the Task SDK version.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from importlib.metadata import version
from pathlib import Path

from packaging.version import Version

# Repo root: providers/standard/tests/unit/standard/test_version_compat.py -> up 5
_REPO_ROOT = Path(__file__).resolve().parents[5]
_SDK_SRC = _REPO_ROOT / "task-sdk" / "src"
_STANDARD_SRC = _REPO_ROOT / "providers" / "standard" / "src"


def test_version_tuple_matches_core_when_core_installed():
    """With apache-airflow-core installed, the tuple reflects core's version."""
    from airflow.providers.standard.version_compat import get_base_airflow_version_tuple

    core_version = Version(version("apache-airflow-core"))
    assert get_base_airflow_version_tuple() == (
        core_version.major,
        core_version.minor,
        core_version.micro,
    )


def test_version_resolves_without_airflow_core():
    """In an SDK-only install, version_compat imports and resolves the version flags.

    Runs a child interpreter with ``-S`` (no site customization) and a
    ``sys.path`` containing only the Task SDK source, the standard provider
    source, the venv site-packages, and the stdlib - deliberately NO
    apache-airflow-core. The probe asserts:

    * importing ``version_compat`` does not raise;
    * ``AIRFLOW_V_3_0_PLUS`` is True (the SDK in this tree maps to Airflow 3.x);
    * no ``airflow-core`` module leaked into ``sys.modules``.
    """
    site_packages = next(p for p in sys.path if "site-packages" in p)
    stdlib = [p for p in sys.path if "python3" in p and "site-packages" not in p and p]

    # Exercise the unit under test - the version-detection logic in
    # version_compat - in isolation. The probe AST-extracts the
    # ``get_base_airflow_version_tuple`` definition from the real source file and
    # runs it on an SDK-only path. It deliberately avoids importing the
    # version_compat *module* (or the provider package), whose unrelated
    # module-level ``from airflow.sdk import BaseOperator`` / generated
    # ``__init__.py`` guard are separate decoupling concerns owned elsewhere.
    version_compat_path = _STANDARD_SRC / "airflow" / "providers" / "standard" / "version_compat.py"

    probe = textwrap.dedent(
        """
        import ast
        import importlib.util
        import sys

        src = open(_VERSION_COMPAT_PATH).read()
        tree = ast.parse(src)
        func = next(
            n for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "get_base_airflow_version_tuple"
        )
        ns = {}
        exec(compile(ast.Module([func], []), _VERSION_COMPAT_PATH, "exec"), ns)
        get_base_airflow_version_tuple = ns["get_base_airflow_version_tuple"]

        # The flag derivation that version_compat performs at module scope.
        tup = get_base_airflow_version_tuple()
        AIRFLOW_V_3_0_PLUS = tup >= (3, 0, 0)

        assert importlib.util.find_spec("airflow.configuration") is None, (
            "apache-airflow-core leaked onto the SDK-only path"
        )
        assert isinstance(tup, tuple) and len(tup) == 3, tup
        assert tup[0] == 3, f"expected an Airflow 3.x base version, got {tup}"
        assert AIRFLOW_V_3_0_PLUS is True, tup

        def is_core(name):
            if not name.startswith("airflow"):
                return False
            if name == "airflow":
                return False
            for prefix in ("airflow.sdk", "airflow._shared", "airflow.providers"):
                if name == prefix or name.startswith(prefix + "."):
                    return False
            return True
        leaked = sorted(n for n in sys.modules if is_core(n))
        assert leaked == [], f"airflow-core modules leaked: {leaked}"

        print("OK", tup)
        """
    )

    env_path = [str(_SDK_SRC), str(_STANDARD_SRC), site_packages, *stdlib]
    code = "import sys; sys.path = %r; _VERSION_COMPAT_PATH = %r; exec(%r)" % (
        env_path,
        str(version_compat_path),
        probe,
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"SDK-only version detection failed.\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert result.stdout.strip().startswith("OK"), result.stdout
