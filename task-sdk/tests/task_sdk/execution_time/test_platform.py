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
"""Tests for the platform abstraction module."""

from __future__ import annotations

import sys

import pytest


class TestPlatformDetection:
    """Test platform detection."""

    def test_is_windows_detection(self):
        """Test that IS_WINDOWS is correctly set based on platform."""
        from airflow.sdk.execution_time.platform import IS_WINDOWS

        expected = sys.platform == "win32"
        assert expected == IS_WINDOWS

    @pytest.mark.unix_only
    def test_unix_signal_handler_available(self):
        """Test that UnixSignalHandler is available on Unix."""
        from airflow.sdk.execution_time.platform import SignalHandler
        from airflow.sdk.execution_time.platform.unix import UnixSignalHandler

        assert SignalHandler is UnixSignalHandler

    @pytest.mark.windows_only
    def test_windows_signal_handler_available(self):
        """Test that WindowsSignalHandler is available on Windows."""
        from airflow.sdk.execution_time.platform import SignalHandler
        from airflow.sdk.execution_time.platform.windows import WindowsSignalHandler

        assert SignalHandler is WindowsSignalHandler


class TestTimeout:
    """Test the timeout module."""

    def test_timeout_class_available(self):
        """Test that the appropriate timeout class is available."""
        from airflow.sdk.execution_time.timeout import timeout

        if sys.platform == "win32":
            from airflow.sdk.execution_time.timeout import TimeoutWindows

            assert timeout is TimeoutWindows
        else:
            from airflow.sdk.execution_time.timeout import TimeoutPosix

            assert timeout is TimeoutPosix

    def test_timeout_context_manager(self):
        """Test that timeout can be used as a context manager."""
        from airflow.sdk.execution_time.timeout import timeout

        # Should not raise
        with timeout(seconds=10):
            pass

    def test_timeout_cancel_on_exit(self):
        """Test that timeout is properly cancelled on exit."""
        from airflow.sdk.execution_time.timeout import timeout

        t = timeout(seconds=10)
        t.__enter__()
        t.__exit__(None, None, None)

        # On Windows, verify timer is cancelled
        if sys.platform == "win32":
            assert t._timer is None


@pytest.mark.unix_only
class TestUnixSignalHandler:
    """Tests for UnixSignalHandler (Unix only)."""

    def test_escalation_path(self):
        """Test that escalation path contains expected signals."""
        import signal

        from airflow.sdk.execution_time.platform.unix import UnixSignalHandler

        assert signal.SIGINT in UnixSignalHandler.ESCALATION_PATH
        assert signal.SIGTERM in UnixSignalHandler.ESCALATION_PATH
        assert signal.SIGKILL in UnixSignalHandler.ESCALATION_PATH

    def test_reset_signals(self):
        """Test that reset_signals doesn't raise."""
        from airflow.sdk.execution_time.platform.unix import UnixSignalHandler

        # Should not raise
        UnixSignalHandler.reset_signals()

    def test_get_signal_name(self):
        """Test signal name lookup."""
        import signal

        from airflow.sdk.execution_time.platform.unix import UnixSignalHandler

        assert UnixSignalHandler.get_signal_name(signal.SIGTERM) == "SIGTERM"
        assert UnixSignalHandler.get_signal_name(signal.SIGKILL) == "SIGKILL"


@pytest.mark.windows_only
class TestWindowsSignalHandler:
    """Tests for WindowsSignalHandler (Windows only)."""

    def test_reset_signals(self):
        """Test that reset_signals doesn't raise on Windows."""
        from airflow.sdk.execution_time.platform.windows import WindowsSignalHandler

        # Should not raise
        WindowsSignalHandler.reset_signals()

    def test_get_signal_name(self):
        """Test exit code name lookup on Windows."""
        from airflow.sdk.execution_time.platform.windows import WindowsSignalHandler

        assert WindowsSignalHandler.get_signal_name(0) == "SUCCESS"
        assert WindowsSignalHandler.get_signal_name(1) == "ERROR"


@pytest.mark.windows_only
class TestWindowsSocketSharing:
    """Tests for Windows socket sharing functions."""

    def test_share_sockets_function_exists(self):
        """Test that share_sockets function is available."""
        from airflow.sdk.execution_time.platform import share_sockets

        assert share_sockets is not None
        assert callable(share_sockets)

    def test_receive_sockets_function_exists(self):
        """Test that receive_sockets function is available."""
        from airflow.sdk.execution_time.platform import receive_sockets

        assert receive_sockets is not None
        assert callable(receive_sockets)


@pytest.mark.windows_only
class TestWindowsUnsupportedFeatureError:
    """Tests for WindowsUnsupportedFeatureError."""

    def test_error_with_known_feature(self):
        """Test error message for known feature."""
        from airflow.sdk.execution_time.platform.windows import WindowsUnsupportedFeatureError

        error = WindowsUnsupportedFeatureError("run_as_user")
        assert "run_as_user" in str(error)
        assert "not supported on Windows" in str(error)

    def test_error_with_unknown_feature(self):
        """Test error message for unknown feature."""
        from airflow.sdk.execution_time.platform.windows import WindowsUnsupportedFeatureError

        error = WindowsUnsupportedFeatureError("unknown_feature")
        assert "unknown_feature" in str(error)

    def test_error_with_custom_message(self):
        """Test error with custom message."""
        from airflow.sdk.execution_time.platform.windows import WindowsUnsupportedFeatureError

        error = WindowsUnsupportedFeatureError("test", custom_message="Custom error message")
        assert str(error) == "Custom error message"
