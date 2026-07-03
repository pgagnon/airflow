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

import importlib
import sys
import warnings

import pytest


def _reimport_shim():
    """Force a fresh import of the deprecated shim module so the import-time warning fires."""
    sys.modules.pop("airflow.sdk.execution_time.secrets_masker", None)
    return importlib.import_module("airflow.sdk.execution_time.secrets_masker")


def test_import_emits_deprecation_warning():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _reimport_shim()

    messages = [str(w.message) for w in caught]
    assert any(
        "airflow.sdk.execution_time.secrets_masker" in m and "deprecated" in m for m in messages
    ), f"Expected a deprecation warning, got: {messages}"
    assert any(issubclass(w.category, DeprecationWarning) for w in caught), (
        f"Expected a DeprecationWarning subclass, got categories: {[w.category for w in caught]}"
    )


def test_import_does_not_pull_in_core_deprecation_tools():
    sys.modules.pop("airflow.utils.deprecation_tools", None)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _reimport_shim()
    assert "airflow.utils.deprecation_tools" not in sys.modules


def test_attribute_access_redirects_to_shared(monkeypatch):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        shim = _reimport_shim()
    import airflow.sdk._shared.secrets_masker as new_module

    assert shim.SecretsMasker is new_module.SecretsMasker


def test_unknown_attribute_raises_attribute_error():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        shim = _reimport_shim()
    with pytest.raises(AttributeError):
        _ = shim.this_attribute_does_not_exist
