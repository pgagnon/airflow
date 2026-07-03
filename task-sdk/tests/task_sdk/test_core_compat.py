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

import builtins

import pytest

from airflow.sdk._core_compat import require_core


@pytest.fixture
def no_core(monkeypatch):
    """Simulate apache-airflow-core not being importable."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "airflow.models" or name.startswith("airflow.models."):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)


def test_require_core_raises_friendly_message_when_core_absent(no_core):
    with pytest.raises(ImportError) as exc_info:
        with require_core("Backfilling"):
            import airflow.models  # noqa: F401

    assert str(exc_info.value) == (
        "Backfilling requires apache-airflow-core. "
        "Install it with: pip install 'apache-airflow-task-sdk[core]'"
    )


def test_require_core_is_transparent_when_import_succeeds():
    sentinel = []
    with require_core("Backfilling"):
        sentinel.append("imported")
    assert sentinel == ["imported"]


def test_require_core_does_not_swallow_unrelated_errors():
    with pytest.raises(ValueError, match="boom"):
        with require_core("Backfilling"):
            raise ValueError("boom")
