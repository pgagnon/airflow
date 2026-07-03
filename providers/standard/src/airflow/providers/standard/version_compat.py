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
#
# NOTE! THIS FILE IS COPIED MANUALLY IN OTHER PROVIDERS DELIBERATELY TO AVOID ADDING UNNECESSARY
# DEPENDENCIES BETWEEN PROVIDERS. IF YOU WANT TO ADD CONDITIONAL CODE IN YOUR PROVIDER THAT DEPENDS
# ON AIRFLOW VERSION, PLEASE COPY THIS FILE TO THE ROOT PACKAGE OF YOUR PROVIDER AND IMPORT
# THOSE CONSTANTS FROM IT RATHER THAN IMPORTING THEM FROM ANOTHER PROVIDER OR TEST CODE
#
from __future__ import annotations


def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    from importlib.metadata import PackageNotFoundError, version

    from packaging.version import Version

    try:
        # Prefer the installed airflow-core distribution. This is the source of
        # truth when core is present and avoids importing ``airflow.__version__``
        # (which does not exist in a core-less, Task-SDK-only install where the
        # ``airflow`` namespace is the SDK's pkgutil shim).
        airflow_version = Version(version("apache-airflow-core"))
    except PackageNotFoundError:
        # SDK-only install: derive the equivalent base Airflow version from the
        # Task SDK. The SDK minor/micro track the Airflow minor/micro and the SDK
        # major is two behind Airflow's (SDK 1.x.y <-> Airflow 3.x.y), so the
        # ``AIRFLOW_V_3_*`` gates stay correct without apache-airflow-core.
        sdk_version = Version(version("apache-airflow-task-sdk"))
        return sdk_version.major + 2, sdk_version.minor, sdk_version.micro
    return airflow_version.major, airflow_version.minor, airflow_version.micro


AIRFLOW_V_3_0_PLUS: bool = get_base_airflow_version_tuple() >= (3, 0, 0)
AIRFLOW_V_3_1_PLUS: bool = get_base_airflow_version_tuple() >= (3, 1, 0)
AIRFLOW_V_3_1_3_PLUS: bool = get_base_airflow_version_tuple() >= (3, 1, 3)
AIRFLOW_V_3_2_PLUS: bool = get_base_airflow_version_tuple() >= (3, 2, 0)
AIRFLOW_V_3_3_PLUS: bool = get_base_airflow_version_tuple() >= (3, 3, 0)

# BaseOperator: Use 3.1+ due to xcom_push method missing in SDK BaseOperator 3.0.x
# This is needed for DecoratedOperator compatibility
if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import BaseOperator
    from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
else:
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]
    from airflow.utils.types import NOTSET, ArgNotSet  # type: ignore[attr-defined,no-redef]

try:
    from airflow.sdk.definitions._internal.types import is_arg_set
except ImportError:

    def is_arg_set(value):  # type: ignore[misc,no-redef]
        return value is not NOTSET


__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "AIRFLOW_V_3_1_PLUS",
    "AIRFLOW_V_3_2_PLUS",
    "AIRFLOW_V_3_3_PLUS",
    "ArgNotSet",
    "BaseOperator",
    "is_arg_set",
]
