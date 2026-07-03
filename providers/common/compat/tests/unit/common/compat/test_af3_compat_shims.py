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
"""Pin the Airflow 3 behavior of the common.compat shims after AF2 dead code removal."""

from __future__ import annotations


def test_notifier_resolves_to_sdk():
    from airflow.sdk.bases.notifier import BaseNotifier as SdkBaseNotifier

    from airflow.providers.common.compat.notifier import BaseNotifier

    assert BaseNotifier is SdkBaseNotifier


def test_permissions_resource_asset_is_assets():
    from airflow.providers.common.compat.security.permissions import RESOURCE_ASSET

    assert RESOURCE_ASSET == "Assets"


def test_sdk_af3_only_exceptions_and_asset_lineage_resolve():
    from airflow.providers.common.compat import sdk

    assert sdk.DownstreamTasksSkipped is not None
    assert sdk.DagRunTriggerException is not None
    assert sdk.AssetLineageInfo is not None


def test_assets_resolve_to_sdk_and_core():
    from airflow.sdk.definitions.asset import Asset as SdkAsset

    from airflow.providers.common.compat.assets import (
        Asset,
        AssetDetails,
        expand_alias_to_assets,
    )

    assert Asset is SdkAsset
    assert AssetDetails is not None
    assert callable(expand_alias_to_assets)
