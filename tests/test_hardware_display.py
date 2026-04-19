"""Unit tests for the per-board HDMI :class:`DisplayProbe` strategies (#178)."""
from __future__ import annotations

import errno
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hardware.display import PortStatus, build_probe
from hardware.display.drm_sysfs import DrmSysfsDisplayProbe
from hardware.display.i2c_edid import I2cEdidDisplayProbe
from hardware.display.null import NullDisplayProbe
from shared.board import Board, HdmiPort


# ── Factory ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "board,expected_cls",
    [
        (Board.PI_5, DrmSysfsDisplayProbe),
        (Board.PI_4, I2cEdidDisplayProbe),
        (Board.ZERO_2W, I2cEdidDisplayProbe),
        (Board.UNKNOWN, NullDisplayProbe),
    ],
)
def test_build_probe_picks_right_strategy(board, expected_cls):
    ports = [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]
    # Prevent I2cEdidDisplayProbe's constructor from actually modprobing.
    with patch("hardware.display.i2c_edid._try_load_i2c_dev"):
        probe = build_probe(board, ports)
    assert isinstance(probe, expected_cls)


# ── NullDisplayProbe ────────────────────────────────────────────────


def test_null_probe_returns_none_for_all_ports():
    ports = [
        HdmiPort("HDMI-0", "/dev/i2c-1", "HDMI-A-1"),
        HdmiPort("HDMI-1", "/dev/i2c-10", "HDMI-A-2"),
    ]
    probe = NullDisplayProbe(ports)
    result = probe.probe_all()
    assert result == [
        PortStatus(name="HDMI-0", connected=None),
        PortStatus(name="HDMI-1", connected=None),
    ]


# ── DrmSysfsDisplayProbe ────────────────────────────────────────────


def _drm_probe(ports):
    return DrmSysfsDisplayProbe(ports)


def test_drm_sysfs_connected(tmp_path):
    card_dir = tmp_path / "card1-HDMI-A-1"
    card_dir.mkdir()
    (card_dir / "status").write_text("connected\n")
    (card_dir / "edid").write_bytes(b"\x00" * 128)

    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1")])
        assert probe.probe_all() == [PortStatus(name="HDMI-0", connected=True)]


def test_drm_sysfs_disconnected(tmp_path):
    card_dir = tmp_path / "card1-HDMI-A-1"
    card_dir.mkdir()
    (card_dir / "status").write_text("disconnected\n")

    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1")])
        assert probe.probe_all() == [PortStatus(name="HDMI-0", connected=False)]


def test_drm_sysfs_status_connected_but_edid_empty_treated_as_disconnected(tmp_path):
    # Pi 5 / VC4 false-positive: sysfs says connected but no display responded
    # on DDC, so the EDID blob is empty. Trust EDID over status.
    card_dir = tmp_path / "card1-HDMI-A-1"
    card_dir.mkdir()
    (card_dir / "status").write_text("connected\n")
    (card_dir / "edid").write_bytes(b"")  # 0 bytes

    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1")])
        assert probe.probe_all() == [PortStatus(name="HDMI-0", connected=False)]


def test_drm_sysfs_status_connected_edid_missing_treated_as_disconnected(tmp_path):
    # Same false-positive case, but the edid file doesn't exist at all.
    card_dir = tmp_path / "card1-HDMI-A-1"
    card_dir.mkdir()
    (card_dir / "status").write_text("connected\n")
    # no edid file created

    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1")])
        assert probe.probe_all() == [PortStatus(name="HDMI-0", connected=False)]


def test_drm_sysfs_unknown_status_is_none(tmp_path):
    card_dir = tmp_path / "card1-HDMI-A-1"
    card_dir.mkdir()
    (card_dir / "status").write_text("unknown\n")

    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1")])
        assert probe.probe_all() == [PortStatus(name="HDMI-0", connected=None)]


def test_drm_sysfs_file_missing_returns_none(tmp_path):
    # No card dir at all.
    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1")])
        assert probe.probe_all() == [PortStatus(name="HDMI-0", connected=None)]


def test_drm_sysfs_empty_connector_returns_none(tmp_path):
    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([HdmiPort("HDMI-0", "/dev/i2c-3", "")])
        assert probe.probe_all() == [PortStatus(name="HDMI-0", connected=None)]


def test_drm_sysfs_multiple_ports(tmp_path):
    (tmp_path / "card1-HDMI-A-1").mkdir()
    (tmp_path / "card1-HDMI-A-1" / "status").write_text("connected")
    (tmp_path / "card1-HDMI-A-1" / "edid").write_bytes(b"\x00" * 256)
    (tmp_path / "card1-HDMI-A-2").mkdir()
    (tmp_path / "card1-HDMI-A-2" / "status").write_text("disconnected")

    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([
            HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1"),
            HdmiPort("HDMI-1", "/dev/i2c-4", "HDMI-A-2"),
        ])
        assert probe.probe_all() == [
            PortStatus(name="HDMI-0", connected=True),
            PortStatus(name="HDMI-1", connected=False),
        ]


def test_drm_sysfs_false_positive_primary_real_secondary(tmp_path):
    # Real-world Pi 5 scenario: HDMI-A-1 sysfs lies (connected, edid empty),
    # HDMI-A-2 honestly reports disconnected. Both should read as disconnected.
    (tmp_path / "card1-HDMI-A-1").mkdir()
    (tmp_path / "card1-HDMI-A-1" / "status").write_text("connected")
    (tmp_path / "card1-HDMI-A-1" / "edid").write_bytes(b"")
    (tmp_path / "card1-HDMI-A-2").mkdir()
    (tmp_path / "card1-HDMI-A-2" / "status").write_text("disconnected")
    (tmp_path / "card1-HDMI-A-2" / "edid").write_bytes(b"")

    with patch("hardware.display.drm_sysfs._SYSFS_ROOT", tmp_path):
        probe = _drm_probe([
            HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1"),
            HdmiPort("HDMI-1", "/dev/i2c-4", "HDMI-A-2"),
        ])
        assert probe.probe_all() == [
            PortStatus(name="HDMI-0", connected=False),
            PortStatus(name="HDMI-1", connected=False),
        ]


# ── I2cEdidDisplayProbe ─────────────────────────────────────────────


def _i2c_probe(ports):
    with patch("hardware.display.i2c_edid._try_load_i2c_dev"):
        return I2cEdidDisplayProbe(ports)


def test_i2c_edid_connected():
    ports = [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]
    fake_fcntl = MagicMock()
    with patch("hardware.display.i2c_edid._fcntl", fake_fcntl), \
         patch("hardware.display.i2c_edid.os.open", return_value=5) as mock_open, \
         patch("hardware.display.i2c_edid.os.read", return_value=b"\x00") as mock_read, \
         patch("hardware.display.i2c_edid.os.close") as mock_close:
        probe = _i2c_probe(ports)
        result = probe.probe_all()
    assert result == [PortStatus(name="HDMI-0", connected=True)]
    mock_open.assert_called_once_with("/dev/i2c-2", 2)  # O_RDWR
    fake_fcntl.ioctl.assert_called_once_with(5, 0x0703, 0x50)
    mock_read.assert_called_once_with(5, 1)
    mock_close.assert_called_once_with(5)


def test_i2c_edid_disconnected_eio():
    ports = [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]
    fake_fcntl = MagicMock()
    with patch("hardware.display.i2c_edid._fcntl", fake_fcntl), \
         patch("hardware.display.i2c_edid.os.open", return_value=5), \
         patch("hardware.display.i2c_edid.os.read",
               side_effect=OSError(errno.EIO, "I/O error")), \
         patch("hardware.display.i2c_edid.os.close") as mock_close:
        probe = _i2c_probe(ports)
        result = probe.probe_all()
    assert result == [PortStatus(name="HDMI-0", connected=False)]
    mock_close.assert_called_once_with(5)


def test_i2c_edid_ioctl_error_disconnected():
    ports = [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]
    fake_fcntl = MagicMock()
    fake_fcntl.ioctl.side_effect = OSError(errno.EINVAL, "Invalid argument")
    with patch("hardware.display.i2c_edid._fcntl", fake_fcntl), \
         patch("hardware.display.i2c_edid.os.open", return_value=5), \
         patch("hardware.display.i2c_edid.os.close") as mock_close:
        probe = _i2c_probe(ports)
        result = probe.probe_all()
    assert result == [PortStatus(name="HDMI-0", connected=False)]
    mock_close.assert_called_once_with(5)


def test_i2c_edid_bus_missing_returns_none():
    ports = [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]
    fake_fcntl = MagicMock()
    with patch("hardware.display.i2c_edid._fcntl", fake_fcntl), \
         patch("hardware.display.i2c_edid.os.open",
               side_effect=OSError(errno.ENOENT, "No such file or directory")):
        probe = _i2c_probe(ports)
        result = probe.probe_all()
    assert result == [PortStatus(name="HDMI-0", connected=None)]


def test_i2c_edid_returns_none_when_fcntl_unavailable():
    """On platforms without fcntl (e.g. Windows) probe returns None."""
    ports = [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]
    with patch("hardware.display.i2c_edid._fcntl", None):
        probe = _i2c_probe(ports)
        assert probe.probe_all() == [PortStatus(name="HDMI-0", connected=None)]


def test_i2c_edid_attempts_modprobe_by_default():
    ports = [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]
    with patch("hardware.display.i2c_edid._try_load_i2c_dev") as mock_load:
        I2cEdidDisplayProbe(ports)
    mock_load.assert_called_once()


def test_i2c_edid_skips_modprobe_when_disabled():
    ports = [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]
    with patch("hardware.display.i2c_edid._try_load_i2c_dev") as mock_load:
        I2cEdidDisplayProbe(ports, auto_load_module=False)
    mock_load.assert_not_called()


def test_i2c_edid_multiple_ports():
    ports = [
        HdmiPort("HDMI-0", "/dev/i2c-1", "HDMI-A-1"),
        HdmiPort("HDMI-1", "/dev/i2c-10", "HDMI-A-2"),
    ]

    # Port 0 open returns fd=5 (connected), port 1 open raises ENOENT (bus missing).
    open_calls = {"/dev/i2c-1": 5, "/dev/i2c-10": OSError(errno.ENOENT, "nope")}

    def fake_open(path, flags):
        value = open_calls[path]
        if isinstance(value, OSError):
            raise value
        return value

    fake_fcntl = MagicMock()
    with patch("hardware.display.i2c_edid._fcntl", fake_fcntl), \
         patch("hardware.display.i2c_edid.os.open", side_effect=fake_open), \
         patch("hardware.display.i2c_edid.os.read", return_value=b"\x00"), \
         patch("hardware.display.i2c_edid.os.close"):
        probe = _i2c_probe(ports)
        result = probe.probe_all()

    assert result == [
        PortStatus(name="HDMI-0", connected=True),
        PortStatus(name="HDMI-1", connected=None),
    ]


# ── Cached factory lifecycle ────────────────────────────────────────


def test_get_display_probe_caches_and_reset():
    from hardware.display import factory as display_factory
    display_factory.reset_cached_probe()
    with patch.object(display_factory, "get_board", return_value=Board.UNKNOWN), \
         patch.object(display_factory, "get_hdmi_ports",
                      return_value=[HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")]):
        probe1 = display_factory.get_display_probe()
        probe2 = display_factory.get_display_probe()
    assert probe1 is probe2
    display_factory.reset_cached_probe()
