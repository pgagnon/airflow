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

# We don't want to `import *` here to avoid the risk of making adding too much to Public python API
from typing import TYPE_CHECKING

from airflow.sdk._shared.timezones import timezone as _timezone
from airflow.sdk._shared.timezones.timezone import (
    coerce_datetime,
    convert_to_utc,
    datetime,
    from_timestamp,
    initialize,
    make_naive,
    parse,
    utc,
    utcnow,
)

if TYPE_CHECKING:
    from pendulum.tz.timezone import FixedTimezone, Timezone

try:
    from airflow.sdk.configuration import conf

    tz_str = conf.get_mandatory_value("core", "default_timezone")
    initialize(tz_str)
except Exception:
    initialize("UTC")


def local_timezone() -> FixedTimezone | Timezone:
    """Return the timezone the SDK was initialized with (the default/local timezone)."""
    return _timezone._Timezone.initialized_timezone


def __getattr__(name: str):
    # ``TIMEZONE`` is the SDK's replacement for ``airflow.settings.TIMEZONE``. It
    # is exposed lazily so it always reflects the currently-initialized timezone.
    if name == "TIMEZONE":
        return _timezone._Timezone.initialized_timezone
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "TIMEZONE",
    "coerce_datetime",
    "convert_to_utc",
    "datetime",
    "from_timestamp",
    "local_timezone",
    "make_naive",
    "parse",
    "utc",
    "utcnow",
]
