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

from typing import TYPE_CHECKING, Any

from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAll, AssetAny

if TYPE_CHECKING:
    # These live only in airflow-core; annotations are import-time-free.
    from airflow.api_fastapi.auth.managers.models.resource_details import (
        AssetAliasDetails as AssetAliasDetails,
        AssetDetails as AssetDetails,
    )
    from airflow.models.asset import expand_alias_to_assets as expand_alias_to_assets


__all__ = [
    "Asset",
    "AssetAlias",
    "AssetAliasDetails",
    "AssetAll",
    "AssetAny",
    "AssetDetails",
    "expand_alias_to_assets",
]

# ``AssetDetails``/``AssetAliasDetails`` (auth-manager resource details) and
# ``expand_alias_to_assets`` (asset DB query) are airflow-core-only with no Task
# SDK equivalent. Resolving them lazily keeps this module import core-free for
# SDK-only installs while preserving the AF3.0 asset-compat surface: only
# *accessing* one of these names pulls airflow-core.
_CORE_ONLY = {
    "AssetAliasDetails": "airflow.api_fastapi.auth.managers.models.resource_details",
    "AssetDetails": "airflow.api_fastapi.auth.managers.models.resource_details",
    "expand_alias_to_assets": "airflow.models.asset",
}


def __getattr__(name: str) -> Any:
    module_path = _CORE_ONLY.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    from airflow.sdk._core_compat import require_core

    with require_core(f"common.compat asset attribute {name!r}"):
        module = importlib.import_module(module_path)
    return getattr(module, name)
