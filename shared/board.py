"""Board detection and hardware capability mapping for Raspberry Pi models."""

from __future__ import annotations

import enum
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("agora.board")

# Device tree model file (standard on all Raspberry Pi boards)
_MODEL_PATH = Path("/proc/device-tree/model")

# ALSA card-list file used to detect attached audio devices at runtime.
# Format example:
#     0 [vc4hdmi0       ]: vc4-hdmi - vc4-hdmi-0
#                          vc4-hdmi-0
#     1 [sndrpihifiberry]: simple-card - snd_rpi_hifiberry_dacplus
#                          snd_rpi_hifiberry_dacplus
_ASOUND_CARDS_PATH = Path("/proc/asound/cards")

# ALSA card name used by the upstream snd_soc_hifiberry_dacplus driver
# (which the InnoMaker PCM5122 HAT binds against via dtoverlay=hifiberry-dacplus).
_HIFIBERRY_CARD = "sndrpihifiberry"


class Board(str, enum.Enum):
    """Supported Raspberry Pi board variants."""
    ZERO_2W = "zero_2w"
    PI_4 = "pi_4"
    PI_5 = "pi_5"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class HdmiPort:
    """HDMI port metadata: I²C DDC bus path and DRM connector name.

    ``drm_connector`` is the name used by the kernel DRM/KMS subsystem
    (e.g. ``HDMI-A-1``, ``HDMI-A-2``).  It corresponds to
    ``/sys/class/drm/card*-{drm_connector}/``.  Not all boards expose a
    DRM sysfs status that's reliable — see ``hardware.display`` for the
    board-aware probe selection.
    """
    name: str               # e.g. "HDMI-0", "HDMI-1"
    i2c_bus: str            # e.g. "/dev/i2c-2"
    drm_connector: str = ""  # e.g. "HDMI-A-1"


# Per-board hardware capabilities
_BOARD_CONFIG: dict[Board, dict] = {
    Board.ZERO_2W: {
        "hdmi_ports": [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")],
        "codecs": ["h264"],
        "has_wifi": True,
        "has_ethernet": False,
        "max_fps": 30,
        "player_backend": "gstreamer",
        "alsa_card": "vc4hdmi",
    },
    Board.PI_4: {
        "hdmi_ports": [
            HdmiPort("HDMI-0", "/dev/i2c-1", "HDMI-A-1"),
            HdmiPort("HDMI-1", "/dev/i2c-10", "HDMI-A-2"),
        ],
        "codecs": ["hevc", "h264"],
        "has_wifi": True,
        "has_ethernet": True,
        "max_fps": 30,
        "player_backend": "mpv",
        "alsa_card": "vc4hdmi",
    },
    Board.PI_5: {
        "hdmi_ports": [
            HdmiPort("HDMI-0", "/dev/i2c-3", "HDMI-A-1"),
            HdmiPort("HDMI-1", "/dev/i2c-4", "HDMI-A-2"),
        ],
        "codecs": ["hevc"],
        "has_wifi": False,  # CM5 has no WiFi; Pi 5 board does — detected at runtime
        "has_ethernet": True,
        "max_fps": 60,
        "player_backend": "mpv",
        "alsa_card": "vc4hdmi0",
    },
}

# Fallback for unknown boards
_UNKNOWN_CONFIG: dict = {
    "hdmi_ports": [HdmiPort("HDMI-0", "/dev/i2c-2", "HDMI-A-1")],
    "codecs": ["h264"],
    "has_wifi": True,
    "has_ethernet": False,
    "max_fps": 30,
    "player_backend": "gstreamer",
    "alsa_card": "vc4hdmi",
}

# Cached board detection result
_cached_board: Board | None = None

# Cached audio-device detection result (card_name, device_prefix).
# Audio cards do not change at runtime, so probe once per process.
_cached_audio_device: tuple[str, str] | None = None


def _read_model_string() -> str:
    """Read the board model string from device tree."""
    try:
        return _MODEL_PATH.read_text().strip().rstrip("\x00")
    except (FileNotFoundError, OSError):
        return ""


def _detect_board(model: str) -> Board:
    """Map a device-tree model string to a Board enum."""
    model_lower = model.lower()
    if "pi 5" in model_lower or "pi5" in model_lower or "compute module 5" in model_lower:
        return Board.PI_5
    if "pi 4" in model_lower or "pi4" in model_lower or "compute module 4" in model_lower:
        return Board.PI_4
    if "pi zero 2" in model_lower:
        return Board.ZERO_2W
    if model:
        logger.warning("Unrecognised board model: %s — using UNKNOWN defaults", model)
    return Board.UNKNOWN


def get_board() -> Board:
    """Detect and return the current board variant (cached after first call)."""
    global _cached_board
    if _cached_board is None:
        model = _read_model_string()
        _cached_board = _detect_board(model)
        logger.info("Detected board: %s (model: %s)", _cached_board.value, model or "N/A")
    return _cached_board


def _config() -> dict:
    """Return the hardware config dict for the current board."""
    board = get_board()
    return _BOARD_CONFIG.get(board, _UNKNOWN_CONFIG)


# ── Public API ──


def get_i2c_bus() -> str:
    """Return the primary (HDMI-0) I2C bus path for display detection."""
    return _config()["hdmi_ports"][0].i2c_bus


def get_i2c_buses() -> list[HdmiPort]:
    """Return all HDMI port I2C bus mappings for the current board."""
    return list(_config()["hdmi_ports"])


def get_hdmi_ports() -> list[HdmiPort]:
    """Return all :class:`HdmiPort` entries for the current board.

    Alias of :func:`get_i2c_buses` with a name that reflects post-#178
    usage (the port carries I²C *and* DRM connector metadata).
    """
    return list(_config()["hdmi_ports"])


def hdmi_port_count() -> int:
    """Return the number of HDMI ports on the current board."""
    return len(_config()["hdmi_ports"])


def supported_codecs() -> list[str]:
    """Return the list of hardware-supported video codecs (e.g. ['hevc', 'h264'])."""
    return list(_config()["codecs"])


def has_wifi() -> bool:
    """Return True if the board has WiFi capability.

    For Pi 5, this checks at runtime whether a WiFi interface exists
    (CM5 has no WiFi, Pi 5 board does).
    """
    board = get_board()
    if board == Board.PI_5:
        return _detect_wifi_interface()
    return _config()["has_wifi"]


def has_ethernet() -> bool:
    """Return True if the board has an Ethernet port."""
    return _config()["has_ethernet"]


def max_fps() -> int:
    """Return the maximum framerate for the current board."""
    return _config()["max_fps"]


def player_backend() -> str:
    """Return the video player backend for the current board.

    Returns 'mpv' for Pi 4 and Pi 5 (uses mpv subprocess with DRM output
    and hardware decoding), or 'gstreamer' for Zero 2 W and unknown boards
    (uses GStreamer pipeline with V4L2 decoder and kmssink).
    """
    return _config()["player_backend"]


def _read_asound_cards() -> str:
    """Read /proc/asound/cards. Returns "" if the file is missing or unreadable.

    Wrapped for easy mocking in tests; not part of the public API.
    """
    try:
        return _ASOUND_CARDS_PATH.read_text()
    except (FileNotFoundError, OSError):
        return ""


def detect_audio_device() -> tuple[str, str]:
    """Return ``(card_name, device_prefix)`` for the active audio output.

    If a HiFiBerry-compatible DAC HAT (e.g. the InnoMaker PCM5122 HAT) is
    present, ``/proc/asound/cards`` will list a ``sndrpihifiberry`` card
    once ``dtoverlay=hifiberry-dacplus`` has registered the driver. In
    that case audio is routed via ALSA ``hw:`` (raw PCM, no HDMI quirks),
    and we return ``("sndrpihifiberry", "hw")``.

    Otherwise we fall back to the per-board HDMI card from
    ``_BOARD_CONFIG`` and a ``hdmi`` device prefix — preserving the
    pre-HAT behaviour on Pis without the DAC.

    Cached after the first call: audio cards do not appear/disappear at
    runtime on these boards.
    """
    global _cached_audio_device
    if _cached_audio_device is not None:
        return _cached_audio_device
    cards = _read_asound_cards()
    if _HIFIBERRY_CARD in cards:
        _cached_audio_device = (_HIFIBERRY_CARD, "hw")
        logger.info("Detected HiFiBerry-compatible DAC HAT (%s); routing audio via ALSA hw:", _HIFIBERRY_CARD)
    else:
        _cached_audio_device = (_config()["alsa_card"], "hdmi")
        logger.info("No DAC HAT detected; routing audio via HDMI card %s", _cached_audio_device[0])
    return _cached_audio_device


def alsa_card() -> str:
    """Return the ALSA card name for the active audio output.

    On boards with a HiFiBerry-compatible DAC HAT this returns
    ``sndrpihifiberry``; otherwise it returns the per-board HDMI card
    name from ``_BOARD_CONFIG``. Kept for backwards compatibility — new
    callers should prefer :func:`alsa_device_string` /
    :func:`alsa_device_string_gst` so the device prefix (``hw`` vs.
    ``hdmi``) is consistent with the card.
    """
    return detect_audio_device()[0]


def alsa_device_string() -> str:
    """Return the mpv-form ALSA device string, e.g. ``alsa/hw:CARD=…,DEV=0``.

    Used for ``mpv --audio-device=…``. Reflects DAC vs. HDMI routing
    based on :func:`detect_audio_device`.
    """
    card, prefix = detect_audio_device()
    return f"alsa/{prefix}:CARD={card},DEV=0"


def alsa_device_string_gst() -> str:
    """Return the GStreamer-form ALSA device string, e.g. ``hw:CARD=…,DEV=0``.

    Used for ``alsasink device=…``. Reflects DAC vs. HDMI routing based
    on :func:`detect_audio_device`.
    """
    card, prefix = detect_audio_device()
    return f"{prefix}:CARD={card},DEV=0"


def _detect_wifi_interface() -> bool:
    """Check whether any WiFi interface exists on the system."""
    try:
        wireless = Path("/proc/net/wireless")
        if wireless.exists():
            # First two lines are header; any additional lines = WiFi interface
            lines = wireless.read_text().strip().splitlines()
            return len(lines) > 2
    except OSError:
        pass
    # Fallback: check sysfs for wireless devices
    try:
        for iface in Path("/sys/class/net").iterdir():
            if (iface / "wireless").is_dir():
                return True
    except OSError:
        pass
    return False


def get_cpu_temp() -> float | None:
    """Read CPU temperature, with vcgencmd fallback to sysfs for Pi 5.

    Returns temperature in Celsius, or None if unavailable.
    """
    # Try vcgencmd first (works on Zero 2 W and Pi 4)
    try:
        result = subprocess.run(
            ["vcgencmd", "measure_temp"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and "temp=" in result.stdout:
            # Output format: "temp=42.3'C"
            temp_str = result.stdout.strip().split("=")[1].rstrip("'C")
            return float(temp_str)
    except (subprocess.SubprocessError, OSError, ValueError, IndexError):
        pass

    # Fallback: sysfs thermal zone (Pi 5, or if vcgencmd unavailable)
    try:
        raw = Path("/sys/class/thermal/thermal_zone0/temp").read_text().strip()
        return int(raw) / 1000.0
    except (FileNotFoundError, OSError, ValueError):
        pass

    return None
