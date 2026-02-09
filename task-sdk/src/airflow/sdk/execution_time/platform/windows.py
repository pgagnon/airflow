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
Windows-specific implementations for the Task SDK execution interface.

This module provides Windows-specific process management using subprocess.Popen
and socket sharing via socket.share()/socket.fromshare(). Process termination
uses psutil's terminate() and kill() methods since Windows doesn't support
Unix signals like SIGINT, SIGTERM, SIGKILL.
"""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psutil


class WindowsUnsupportedFeatureError(Exception):
    """Raised when attempting to use a Unix-only feature on Windows."""

    FEATURE_MESSAGES = {
        "run_as_user": (
            "User impersonation via 'run_as_user' is not supported on Windows. "
            "Consider configuring Windows services to run as the desired user."
        ),
        "send_fds": (
            "Dynamic file descriptor passing via send_fds() is not supported on Windows. "
            "Socket sharing is handled via socket.share()/socket.fromshare() at process startup."
        ),
        "signal_escalation": (
            "Unix signal escalation (SIGINT -> SIGTERM -> SIGKILL) is not supported on Windows. "
            "Process termination uses terminate() followed by kill()."
        ),
    }

    def __init__(self, feature: str, custom_message: str | None = None):
        message = custom_message or self.FEATURE_MESSAGES.get(
            feature,
            f"Feature '{feature}' is not supported on Windows.",
        )
        super().__init__(message)
        self.feature = feature


class WindowsSignalHandler:
    """Handle process termination on Windows systems."""

    @classmethod
    def reset_signals(cls) -> None:
        """
        Reset signal handlers on Windows.

        On Windows, this is mostly a no-op since Windows doesn't support
        the same signal mechanisms as Unix. We only reset the exception hook.
        """
        import sys

        sys.excepthook = sys.__excepthook__
        # Note: Windows doesn't support SIGINT, SIGTERM, SIGUSR2 signal handlers
        # in the same way Unix does. Process termination is handled via
        # terminate()/kill() methods.

    @classmethod
    def kill_process(
        cls,
        process: psutil.Process,
        escalation_delay: float = 5.0,
    ) -> int | None:
        """
        Terminate a process on Windows.

        Uses psutil's terminate() for graceful termination, then kill()
        for forceful termination if the process doesn't exit within
        escalation_delay seconds.

        Args:
            process: The psutil.Process to terminate
            escalation_delay: Seconds to wait before forceful termination

        Returns:
            Exit code if process terminated, None if still running
        """
        import psutil

        try:
            # First, try graceful termination
            process.terminate()

            try:
                exit_code = process.wait(timeout=escalation_delay)
                return exit_code
            except psutil.TimeoutExpired:
                # Process didn't terminate gracefully, force kill
                process.kill()
                try:
                    exit_code = process.wait(timeout=1.0)
                    return exit_code
                except psutil.TimeoutExpired:
                    return None

        except psutil.NoSuchProcess:
            # Process already terminated
            return None

    @classmethod
    def get_signal_name(cls, sig: int) -> str:
        """
        Get human-readable name for a signal/exit code on Windows.

        Windows doesn't use Unix signals, so this interprets exit codes
        differently.
        """
        # Common Windows exit codes
        windows_exit_codes = {
            0: "SUCCESS",
            1: "ERROR",
            -1: "TERMINATED",
            -1073741510: "CTRL_C_EVENT",  # 0xC000013A
            -1073741819: "ACCESS_VIOLATION",  # 0xC0000005
        }
        return windows_exit_codes.get(sig, f"exit code {sig}")


def share_sockets(sockets: list[socket.socket], target_pid: int) -> list[bytes]:
    """
    Share sockets with a target process on Windows.

    Uses Windows-specific socket.share() to create serialized socket data
    that can be passed to a child process.

    Args:
        sockets: List of sockets to share
        target_pid: Process ID that will receive the sockets

    Returns:
        List of serialized socket data (bytes) for each socket
    """
    return [sock.share(target_pid) for sock in sockets]


def receive_sockets(socket_data_list: list[bytes]) -> list[socket.socket]:
    """
    Recreate sockets from shared data on Windows.

    Uses Windows-specific socket.fromshare() to recreate sockets from
    serialized data passed from the parent process.

    Args:
        socket_data_list: List of serialized socket data from share_sockets()

    Returns:
        List of recreated socket objects
    """
    return [socket.fromshare(data) for data in socket_data_list]
