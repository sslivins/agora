"""Tests for shared.board — board detection and hardware capability mapping."""

from unittest.mock import patch, MagicMock

import pytest

from shared.board import (
    Board,
    HdmiPort,
    _detect_board,
    get_board,
    get_cpu_temp,
    get_i2c_bus,
    get_i2c_buses,
    has_ethernet,
    has_wifi,
    hdmi_port_count,
    max_fps,
    player_backend,
    supported_codecs,
)

import shared.board as board_module


@pytest.fixture(autouse=True)
def _reset_cache():
    """Reset the cached board detection before each test."""
    board_module._cached_board = None
    yield
    board_module._cached_board = None


# ── Board detection from model string ──


class TestDetectBoard:
    def test_zero_2w(self):
        assert _detect_board("Raspberry Pi Zero 2 W Rev 1.0") == Board.ZERO_2W

    def test_pi_4(self):
        assert _detect_board("Raspberry Pi 4 Model B Rev 1.5") == Board.PI_4

    def test_pi_4_lowercase(self):
        assert _detect_board("raspberry pi 4 model b") == Board.PI_4

    def test_pi_5(self):
        assert _detect_board("Raspberry Pi 5 Model B Rev 1.0") == Board.PI_5

    def test_pi_5_compute_module(self):
        assert _detect_board("Raspberry Pi 5") == Board.PI_5

    def test_cm5_lite(self):
        assert _detect_board("Raspberry Pi Compute Module 5 Lite Rev 1.0") == Board.PI_5

    def test_cm5(self):
        assert _detect_board("Raspberry Pi Compute Module 5 Rev 1.0") == Board.PI_5

    def test_cm4(self):
        assert _detect_board("Raspberry Pi Compute Module 4 Rev 1.0") == Board.PI_4

    def test_unknown_board(self):
        assert _detect_board("Something Else Entirely") == Board.UNKNOWN

    def test_empty_string(self):
        assert _detect_board("") == Board.UNKNOWN


# ── get_board() with mocked device tree ──


class TestGetBoard:
    def test_reads_from_device_tree(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 4 Model B Rev 1.5"):
            assert get_board() == Board.PI_4

    def test_caches_result(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5") as mock:
            get_board()
            get_board()
            mock.assert_called_once()

    def test_returns_unknown_when_file_missing(self):
        with patch.object(board_module, "_read_model_string", return_value=""):
            assert get_board() == Board.UNKNOWN


# ── I2C bus mapping ──


class TestI2CBus:
    def test_zero_2w_primary(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi Zero 2 W Rev 1.0"):
            assert get_i2c_bus() == "/dev/i2c-2"

    def test_pi_4_primary(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 4 Model B"):
            assert get_i2c_bus() == "/dev/i2c-1"

    def test_pi_5_primary(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            assert get_i2c_bus() == "/dev/i2c-3"

    def test_zero_2w_single_port(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi Zero 2 W"):
            buses = get_i2c_buses()
            assert len(buses) == 1
            assert buses[0] == HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")

    def test_pi_4_dual_ports(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 4 Model B"):
            buses = get_i2c_buses()
            assert len(buses) == 2
            assert buses[0] == HdmiPort("HDMI-0", "/dev/i2c-1", "HDMI-A-1")
            assert buses[1] == HdmiPort("HDMI-1", "/dev/i2c-10", "HDMI-A-2")

    def test_pi_5_dual_ports(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            buses = get_i2c_buses()
            assert len(buses) == 2
            assert buses[0] == HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1")
            assert buses[1] == HdmiPort("HDMI-1", "/dev/i2c-4", "HDMI-A-2")


# ── HDMI port count ──


class TestHdmiPortCount:
    def test_zero_2w_one_port(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi Zero 2 W"):
            assert hdmi_port_count() == 1

    def test_pi_4_two_ports(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 4 Model B"):
            assert hdmi_port_count() == 2

    def test_pi_5_two_ports(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            assert hdmi_port_count() == 2


# ── Codec support ──


class TestSupportedCodecs:
    def test_zero_2w_h264_only(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi Zero 2 W"):
            codecs = supported_codecs()
            assert codecs == ["h264"]

    def test_pi_4_hevc_and_h264(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 4 Model B"):
            codecs = supported_codecs()
            assert codecs == ["hevc", "h264"]

    def test_pi_5_hevc_only(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            codecs = supported_codecs()
            assert codecs == ["hevc"]


# ── WiFi / Ethernet detection ──


class TestNetworkCapabilities:
    def test_zero_2w_has_wifi_no_ethernet(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi Zero 2 W"):
            assert has_wifi() is True
            assert has_ethernet() is False

    def test_pi_4_has_both(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 4 Model B"):
            assert has_wifi() is True
            assert has_ethernet() is True

    def test_pi_5_has_ethernet(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            assert has_ethernet() is True

    def test_pi_5_wifi_runtime_detected_present(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            with patch.object(board_module, "_detect_wifi_interface", return_value=True):
                assert has_wifi() is True

    def test_pi_5_wifi_runtime_detected_absent(self):
        """Pi 5 CM5 has no WiFi — runtime detection should return False."""
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            with patch.object(board_module, "_detect_wifi_interface", return_value=False):
                assert has_wifi() is False


# ── Max FPS ──


class TestMaxFps:
    def test_zero_2w_30fps(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi Zero 2 W"):
            assert max_fps() == 30

    def test_pi_4_30fps(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 4 Model B"):
            assert max_fps() == 30

    def test_pi_5_60fps(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            assert max_fps() == 60


# ── CPU temperature (with sysfs fallback) ──


class TestGetCpuTemp:
    def test_vcgencmd_success(self):
        result = MagicMock()
        result.returncode = 0
        result.stdout = "temp=45.6'C\n"
        with patch("shared.board.subprocess.run", return_value=result):
            assert get_cpu_temp() == 45.6

    def test_vcgencmd_fails_sysfs_fallback(self):
        """When vcgencmd fails, fall back to sysfs thermal zone."""
        with patch("shared.board.subprocess.run", side_effect=FileNotFoundError):
            with patch.object(board_module.Path, "read_text", return_value="42300\n"):
                temp = get_cpu_temp()
                assert temp == 42.3

    def test_returns_none_when_both_fail(self):
        with patch("shared.board.subprocess.run", side_effect=FileNotFoundError):
            with patch.object(board_module.Path, "read_text", side_effect=FileNotFoundError):
                assert get_cpu_temp() is None

    def test_vcgencmd_zero_temp(self):
        result = MagicMock()
        result.returncode = 0
        result.stdout = "temp=0.0'C\n"
        with patch("shared.board.subprocess.run", return_value=result):
            assert get_cpu_temp() == 0.0

    def test_vcgencmd_nonzero_exit(self):
        """Non-zero exit falls through to sysfs."""
        result = MagicMock()
        result.returncode = 1
        result.stdout = ""
        with patch("shared.board.subprocess.run", return_value=result):
            # sysfs will also fail on Windows, so expect None
            assert get_cpu_temp() is None


# ── Player backend ──


class TestPlayerBackend:
    def test_zero_2w_uses_gstreamer(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi Zero 2 W"):
            assert player_backend() == "gstreamer"

    def test_pi_4_uses_mpv(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 4 Model B"):
            assert player_backend() == "mpv"

    def test_pi_5_uses_mpv(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi 5"):
            assert player_backend() == "mpv"

    def test_cm5_uses_mpv(self):
        with patch.object(board_module, "_read_model_string", return_value="Raspberry Pi Compute Module 5"):
            assert player_backend() == "mpv"

    def test_unknown_board_uses_gstreamer(self):
        with patch.object(board_module, "_read_model_string", return_value="Something Else"):
            assert player_backend() == "gstreamer"
