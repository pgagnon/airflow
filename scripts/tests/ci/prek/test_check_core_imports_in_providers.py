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

import textwrap
from pathlib import Path

import pytest
from check_core_imports_in_providers import (
    NOCHECK_CODE,
    check_file_for_core_imports,
    find_module_level_core_imports,
)


class TestFindModuleLevelCoreImports:
    """Detection only looks at module-level imports of airflow-core."""

    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param(
                "from airflow.models import DagRun\n",
                [(1, "from airflow.models import DagRun")],
                id="module-level-from-import-core",
            ),
            pytest.param(
                "import airflow.models\n",
                [(1, "import airflow.models")],
                id="module-level-import-core",
            ),
            pytest.param(
                "import airflow.models as models\n",
                [(1, "import airflow.models as models")],
                id="module-level-import-core-aliased",
            ),
            pytest.param(
                "from airflow import settings\n",
                [(1, "from airflow import settings")],
                id="from-airflow-import-name",
            ),
            pytest.param(
                "from airflow.sdk import DAG\n",
                [],
                id="sdk-import-allowed",
            ),
            pytest.param(
                "from airflow.sdk.definitions import dag\n",
                [],
                id="sdk-submodule-allowed",
            ),
            pytest.param(
                "import airflow.sdk\n",
                [],
                id="import-sdk-allowed",
            ),
            pytest.param(
                "from airflow.providers.common.sql.hooks import DbApiHook\n",
                [],
                id="providers-import-allowed",
            ),
            pytest.param(
                "import airflow.providers.standard\n",
                [],
                id="import-providers-allowed",
            ),
            pytest.param(
                "from airflow import sdk\n",
                [],
                id="from-airflow-import-sdk-allowed",
            ),
            pytest.param(
                "import os\nimport sys\n",
                [],
                id="stdlib-only",
            ),
        ],
    )
    def test_module_level_imports(self, write_python_file, code: str, expected: list[tuple[int, str]]):
        assert find_module_level_core_imports(write_python_file(code)) == expected

    def test_type_checking_import_not_flagged(self, write_python_file):
        code = """
            from __future__ import annotations

            from typing import TYPE_CHECKING

            if TYPE_CHECKING:
                from airflow.models import DagRun
        """
        assert find_module_level_core_imports(write_python_file(code)) == []

    def test_function_local_import_not_flagged(self, write_python_file):
        code = """
            def get_dag_run():
                from airflow.models import DagRun

                return DagRun
        """
        assert find_module_level_core_imports(write_python_file(code)) == []

    def test_noqa_suppresses(self, write_python_file):
        code = f"from airflow.models import DagRun  # noqa: {NOCHECK_CODE}\n"
        assert find_module_level_core_imports(write_python_file(code)) == []


def _make_provider_file(tmp_path: Path, provider_id: str, code: str) -> Path:
    """Create providers/<provider_id-as-dirs>/src/.../hooks.py with provider.yaml."""
    provider_dir = tmp_path / "providers" / Path(*provider_id.split("."))
    src_dir = provider_dir / "src" / "airflow" / "providers" / Path(*provider_id.split("."))
    src_dir.mkdir(parents=True, exist_ok=True)
    (provider_dir / "provider.yaml").touch()
    file_path = src_dir / "hooks.py"
    file_path.write_text(code)
    return file_path


class TestOptIn:
    def test_opted_in_provider_is_flagged(self, tmp_path, monkeypatch):
        import check_core_imports_in_providers as mod

        f = _make_provider_file(tmp_path, "standard", "from airflow.models import DagRun\n")
        monkeypatch.setattr(mod, "OPTED_IN_PROVIDERS", {"standard"})
        assert mod.check_file_for_core_imports(f) == [(1, "from airflow.models import DagRun")]

    def test_non_opted_in_provider_is_skipped(self, tmp_path, monkeypatch):
        import check_core_imports_in_providers as mod

        f = _make_provider_file(tmp_path, "standard", "from airflow.models import DagRun\n")
        monkeypatch.setattr(mod, "OPTED_IN_PROVIDERS", set())
        assert mod.check_file_for_core_imports(f) == []

    def test_opted_in_provider_sdk_import_allowed(self, tmp_path, monkeypatch):
        import check_core_imports_in_providers as mod

        f = _make_provider_file(tmp_path, "standard", "from airflow.sdk import DAG\n")
        monkeypatch.setattr(mod, "OPTED_IN_PROVIDERS", {"standard"})
        assert mod.check_file_for_core_imports(f) == []

    def test_dotted_provider_id_opt_in(self, tmp_path, monkeypatch):
        import check_core_imports_in_providers as mod

        f = _make_provider_file(tmp_path, "common.sql", "import airflow.models\n")
        monkeypatch.setattr(mod, "OPTED_IN_PROVIDERS", {"common.sql"})
        assert mod.check_file_for_core_imports(f) == [(1, "import airflow.models")]

    def test_allowlist_starts_empty(self):
        """The shipped allowlist must stay empty so the hook is a no-op today."""
        import check_core_imports_in_providers as mod

        assert mod.OPTED_IN_PROVIDERS == set()
