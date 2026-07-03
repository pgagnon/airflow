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

import os

from airflow.sdk import plugins_manager


def test_plugins_folder_read_from_conf(monkeypatch):
    """The plugins folder must be sourced from ``conf``, not ``airflow.settings``."""
    monkeypatch.setenv("AIRFLOW__CORE__PLUGINS_FOLDER", "/tmp/custom_plugins")
    assert plugins_manager._get_plugins_folder() == "/tmp/custom_plugins"


def test_plugins_folder_falls_back_to_airflow_home(monkeypatch):
    """With no override, the folder defaults to ``$AIRFLOW_HOME/plugins`` (no core needed)."""
    from airflow.sdk.configuration import conf

    monkeypatch.delenv("AIRFLOW__CORE__PLUGINS_FOLDER", raising=False)
    had_option = conf.has_option("core", "plugins_folder")
    original = conf.get("core", "plugins_folder", fallback=None) if had_option else None
    if had_option:
        conf.remove_option("core", "plugins_folder")
    monkeypatch.setenv("AIRFLOW_HOME", "/tmp/sdk_home")
    try:
        expected = os.path.join("/tmp/sdk_home", "plugins")
        assert plugins_manager._get_plugins_folder() == expected
    finally:
        if had_option:
            conf.set("core", "plugins_folder", original)


def test_plugins_manager_has_no_module_level_airflow_core_import():
    """plugins_manager must not bind ``airflow.settings`` (or any core module) at module level."""
    # The module no longer keeps a top-level reference to airflow-core's settings.
    assert not hasattr(plugins_manager, "settings")


def test_plugins_manager_source_has_no_settings_import():
    """Guard against re-introducing ``from airflow import settings`` at module scope."""
    import inspect

    source = inspect.getsource(plugins_manager)
    # No top-level import of airflow-core (only airflow.sdk.* allowed).
    offending = [
        line
        for line in source.splitlines()
        if (line.startswith("from airflow ") or line.startswith("from airflow.") or line.startswith("import airflow"))
        and "airflow.sdk" not in line
    ]
    assert offending == [], offending
