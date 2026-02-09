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

import ctypes
import os
import sys
import threading
from typing import Any

import structlog

from airflow.sdk.exceptions import AirflowTaskTimeout


class TimeoutPosix:
    """POSIX Timeout version: To be used in a ``with`` block and timeout its content."""

    def __init__(self, seconds=1, error_message="Timeout"):
        super().__init__()
        self.seconds = seconds
        self.error_message = error_message + ", PID: " + str(os.getpid())
        self.log = structlog.get_logger(logger_name="task")

    def handle_timeout(self, signum, frame):
        """Log information and raises AirflowTaskTimeout."""
        self.log.error("Process timed out", pid=os.getpid())
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        import signal

        try:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.setitimer(signal.ITIMER_REAL, self.seconds)
        except ValueError:
            self.log.warning("timeout can't be used in the current context", exc_info=True)
        return self

    def __exit__(self, type_, value, traceback):
        import signal

        try:
            signal.setitimer(signal.ITIMER_REAL, 0)
        except ValueError:
            self.log.warning("timeout can't be used in the current context", exc_info=True)


class TimeoutWindows:
    """
    Windows Timeout version: To be used in a ``with`` block and timeout its content.

    Since Windows doesn't support SIGALRM or setitimer, this implementation uses
    a background thread with threading.Timer to enforce the timeout. When the
    timeout expires, it injects an AirflowTaskTimeout exception into the main
    thread using ctypes.

    Note: This approach has limitations compared to Unix signals:
    - The exception can only be raised when Python is executing Python bytecode
    - Long-running C extensions may not be interrupted immediately
    - The exception may be delayed if the main thread is blocked in a system call
    """

    def __init__(self, seconds: float = 1, error_message: str = "Timeout"):
        super().__init__()
        self.seconds = seconds
        self.error_message = error_message + ", PID: " + str(os.getpid())
        self.log = structlog.get_logger(logger_name="task")
        self._timer: threading.Timer | None = None
        self._main_thread_id: int | None = None
        self._timed_out: bool = False

    def _timeout_handler(self) -> None:
        """Handle timeout expiration from the timer thread."""
        self._timed_out = True
        self.log.error("Process timed out", pid=os.getpid())

        if self._main_thread_id is None:
            return

        # Inject exception into the main thread using ctypes
        # This uses the undocumented but widely-used PyThreadState_SetAsyncExc
        exc_type = ctypes.py_object(AirflowTaskTimeout)
        thread_id = ctypes.c_ulong(self._main_thread_id)

        result = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, exc_type)

        if result == 0:
            self.log.error(
                "Failed to inject timeout exception: invalid thread id",
                thread_id=self._main_thread_id,
            )
        elif result > 1:
            # More than one thread affected - this shouldn't happen
            # Clear the exception to be safe
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, None)
            self.log.error(
                "Failed to inject timeout exception: affected multiple threads",
                thread_id=self._main_thread_id,
                affected_count=result,
            )

    def __enter__(self) -> TimeoutWindows:
        self._main_thread_id = threading.current_thread().ident
        self._timed_out = False

        if self.seconds > 0:
            self._timer = threading.Timer(self.seconds, self._timeout_handler)
            self._timer.daemon = True  # Don't prevent process exit
            self._timer.start()

        return self

    def __exit__(self, type_: type | None, value: Any, traceback: Any) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        # If we timed out but the exception wasn't raised (e.g., caught somewhere),
        # we should still raise it here to ensure the timeout is enforced
        if self._timed_out and type_ is None:
            raise AirflowTaskTimeout(self.error_message)


# Platform selection: use POSIX implementation on Unix, Windows implementation on Windows
if sys.platform == "win32":
    timeout = TimeoutWindows
else:
    timeout = TimeoutPosix
