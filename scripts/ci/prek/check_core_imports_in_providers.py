#!/usr/bin/env python
#
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Flag module-level imports of airflow-core in opted-in providers.

A migrated provider must depend only on the Task SDK (``airflow.sdk``) and on
other providers (``airflow.providers``), never on airflow-core at import time.
This check parses each provider source file and reports any *module-level*
import of an ``airflow.<x>`` module where ``<x>`` is neither ``sdk`` nor
``providers``. Imports inside ``if TYPE_CHECKING:`` blocks and inside
functions/methods are not flagged: they don't run when the module is imported,
so they don't create a runtime dependency on airflow-core.

Opt-in: only providers listed in ``OPTED_IN_PROVIDERS`` are checked. The list
starts empty, so the hook is a no-op today. Phase 2 adds a provider's id (e.g.
``standard``, ``common.sql``) to the list as it is migrated to SDK-only.

Escape hatch: append ``# noqa: PRV001`` to an import line (or the opening/
closing paren line of a multi-line import) to suppress a single intentional
import.
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

from common_prek_utils import get_provider_id_from_path, has_nocheck_marker

NOCHECK_CODE = "PRV001"

# Providers that have been migrated to SDK-only and therefore must not import
# airflow-core at module level. Add a provider id here (matching its
# ``provider.yaml`` location, e.g. "standard", "common.sql") once it is
# migrated in Phase 2. Keep empty until then so this check stays a no-op.
OPTED_IN_PROVIDERS: set[str] = {"common.compat", "common.sql", "http", "smtp", "standard"}


def _is_core_module(module: str) -> bool:
    """True for ``airflow.<x>`` modules that belong to airflow-core.

    ``airflow.sdk`` (the Task SDK) and ``airflow.providers`` (other providers)
    are allowed; everything else under ``airflow.`` is airflow-core.
    """
    if not module.startswith("airflow."):
        return False
    return not (module.startswith("airflow.sdk") or module.startswith("airflow.providers"))


def find_module_level_core_imports(file_path: Path) -> list[tuple[int, str]]:
    """Return ``(lineno, statement)`` for each module-level airflow-core import.

    Only statements directly in the module body are inspected, so imports
    guarded by ``if TYPE_CHECKING:`` or nested in functions are ignored.
    ``# noqa: PRV001`` on the import suppresses it.
    """
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    source_lines = source.splitlines()
    violations: list[tuple[int, str]] = []

    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            if not node.module:
                continue
            if _is_core_module(node.module):
                violating_names = [alias.name for alias in node.names]
            else:
                # Catch ``from airflow import settings`` style imports where the
                # offending module is the dotted ``<module>.<name>`` path.
                violating_names = [
                    alias.name for alias in node.names if _is_core_module(f"{node.module}.{alias.name}")
                ]
            if not violating_names:
                continue
            if has_nocheck_marker(source_lines, node, NOCHECK_CODE):
                continue
            statement = f"from {node.module} import {', '.join(violating_names)}"
            violations.append((node.lineno, statement))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if not _is_core_module(alias.name):
                    continue
                if has_nocheck_marker(source_lines, node, NOCHECK_CODE):
                    continue
                statement = f"import {alias.name}"
                if alias.asname:
                    statement += f" as {alias.asname}"
                violations.append((node.lineno, statement))

    return violations


def check_file_for_core_imports(file_path: Path) -> list[tuple[int, str]]:
    """Check an opted-in provider file for module-level airflow-core imports.

    Files outside the opt-in allowlist (``OPTED_IN_PROVIDERS``) return ``[]`` so
    non-migrated providers are skipped entirely.
    """
    provider_id = get_provider_id_from_path(file_path)
    if provider_id is None or provider_id not in OPTED_IN_PROVIDERS:
        return []
    return find_module_level_core_imports(file_path)


def main():
    parser = argparse.ArgumentParser(
        description="Check for module-level airflow-core imports in opted-in providers"
    )
    parser.add_argument("files", nargs="*", help="Files to check")
    args = parser.parse_args()

    if not args.files:
        return

    # Imported lazily so the script's `# /// script` block stays minimal and the
    # detection helpers above can be unit-tested without rich on the path.
    from common_prek_utils import report_import_violations

    report_import_violations(
        args.files,
        check_func=check_file_for_core_imports,
        violation_label="module-level airflow-core import(s) in opted-in providers",
        nocheck_code=NOCHECK_CODE,
    )


if __name__ == "__main__":
    main()
    sys.exit(0)
