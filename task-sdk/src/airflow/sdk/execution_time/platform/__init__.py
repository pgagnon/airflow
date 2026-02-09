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
"""
Platform abstraction layer for the Task SDK execution interface.

This module provides cross-platform implementations for process management,
signal handling, and IPC mechanisms. On Unix systems, it uses fork-based
process creation and Unix signals. On Windows, it uses subprocess spawning
with socket sharing.
"""

from __future__ import annotations

import sys

IS_WINDOWS = sys.platform == "win32"

if IS_WINDOWS:
    from airflow.sdk.execution_time.platform.windows import (
        WindowsSignalHandler as SignalHandler,
        receive_sockets,
        share_sockets,
    )
else:
    from airflow.sdk.execution_time.platform.unix import (
        UnixSignalHandler as SignalHandler,
    )

    # Unix doesn't need socket sharing - uses fork with inherited fds
    receive_sockets = None
    share_sockets = None

__all__ = [
    "IS_WINDOWS",
    "SignalHandler",
    "share_sockets",
    "receive_sockets",
]
