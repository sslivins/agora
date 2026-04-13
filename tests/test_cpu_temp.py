"""Tests for CPU temperature reading helper."""

from unittest.mock import patch, MagicMock

import subprocess

from cms_client.service import _get_cpu_temp
import shared.board as board_module


class TestGetCpuTemp:
    def test_parses_normal_output(self):
        result = MagicMock()
        result.returncode = 0
        result.stdout = "temp=45.6'C\n"
        with patch("shared.board.subprocess.run", return_value=result) as mock_run:
            temp = _get_cpu_temp()
            assert temp == 45.6
            mock_run.assert_called_once_with(
                ["vcgencmd", "measure_temp"],
                capture_output=True, text=True, timeout=5,
            )

    def test_parses_high_temp(self):
        result = MagicMock()
        result.returncode = 0
        result.stdout = "temp=82.1'C\n"
        with patch("shared.board.subprocess.run", return_value=result):
            assert _get_cpu_temp() == 82.1

    def test_parses_zero_temp(self):
        result = MagicMock()
        result.returncode = 0
        result.stdout = "temp=0.0'C\n"
        with patch("shared.board.subprocess.run", return_value=result):
            assert _get_cpu_temp() == 0.0

    def test_returns_none_on_nonzero_exit(self):
        result = MagicMock()
        result.returncode = 1
        result.stdout = ""
        with patch("shared.board.subprocess.run", return_value=result):
            assert _get_cpu_temp() is None

    def test_returns_none_when_vcgencmd_not_found(self):
        with patch("shared.board.subprocess.run", side_effect=FileNotFoundError):
            assert _get_cpu_temp() is None

    def test_returns_none_on_oserror(self):
        with patch("shared.board.subprocess.run", side_effect=OSError("no such file")):
            assert _get_cpu_temp() is None

    def test_returns_none_on_timeout(self):
        with patch("shared.board.subprocess.run", side_effect=subprocess.TimeoutExpired("vcgencmd", 5)):
            assert _get_cpu_temp() is None

    def test_returns_none_on_malformed_output(self):
        result = MagicMock()
        result.returncode = 0
        result.stdout = "garbage output\n"
        with patch("shared.board.subprocess.run", return_value=result):
            assert _get_cpu_temp() is None

    def test_returns_none_on_empty_output(self):
        result = MagicMock()
        result.returncode = 0
        result.stdout = ""
        with patch("shared.board.subprocess.run", return_value=result):
            assert _get_cpu_temp() is None
