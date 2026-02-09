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
Unix-specific implementations for the Task SDK execution interface.

This module provides Unix-specific process management using fork() and
Unix signals (SIGINT, SIGTERM, SIGKILL) for process termination.
"""

from __future__ import annotations

import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psutil


class UnixSignalHandler:
    """Handle process signaling and termination on Unix systems."""

    # Signal escalation path for Unix: SIGINT -> SIGTERM -> SIGKILL
    ESCALATION_PATH: list[signal.Signals] = [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]

    @classmethod
    def reset_signals(cls) -> None:
        """Reset signal handlers to default in child process after fork."""
        import sys

        sys.excepthook = sys.__excepthook__
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGUSR2, signal.SIG_DFL)

    @classmethod
    def kill_process(
        cls,
        process: psutil.Process,
        signal_to_send: signal.Signals = signal.SIGINT,
        escalation_delay: float = 5.0,
    ) -> int | None:
        """
        Terminate a process with signal escalation on Unix.

        Sends signals in escalation order (SIGINT -> SIGTERM -> SIGKILL),
        waiting escalation_delay seconds between each signal.

        Args:
            process: The psutil.Process to terminate
            signal_to_send: Initial signal to send (default SIGINT)
            escalation_delay: Seconds to wait between escalation signals

        Returns:
            Exit code if process terminated, None if still running
        """
        import time

        import psutil

        # Determine where in escalation path to start
        try:
            start_idx = cls.ESCALATION_PATH.index(signal_to_send)
        except ValueError:
            start_idx = 0

        for sig in cls.ESCALATION_PATH[start_idx:]:
            try:
                process.send_signal(sig)

                start = time.monotonic()
                end = start + escalation_delay

                while time.monotonic() < end:
                    try:
                        exit_code = process.wait(timeout=0.1)
                        return exit_code
                    except psutil.TimeoutExpired:
                        continue

            except psutil.NoSuchProcess:
                # Process already terminated
                return None

        return None

    @classmethod
    def get_signal_name(cls, sig: int) -> str:
        """Get human-readable name for a signal number."""
        try:
            return signal.Signals(sig).name
        except ValueError:
            return f"signal {sig}"
