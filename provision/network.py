"""Network management helpers using NetworkManager via nmcli."""

import logging
import re
import subprocess
from dataclasses import dataclass

logger = logging.getLogger("agora.provision.network")


@dataclass
class WifiNetwork:
    ssid: str
    signal: int  # 0-100
    security: str  # e.g. "WPA2", "WPA3", "OWE", ""


def get_device_serial_suffix(length: int = 4) -> str:
    """Return last N hex chars of the Pi serial number for unique SSID."""
    try:
        with open("/sys/firmware/devicetree/base/serial-number") as f:
            serial = f.read().strip().strip("\x00")
            return serial[-length:].upper()
    except OSError:
        pass
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("Serial"):
                serial = line.split(":")[1].strip()
                return serial[-length:].upper()
    except OSError:
        pass
    return "0000"


def _run(args: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    return subprocess.run(
        args, capture_output=True, text=True, timeout=timeout,
    )


def get_wifi_interface() -> str | None:
    """Return the name of the first Wi-Fi interface, or None."""
    try:
        result = _run(["nmcli", "-t", "-f", "TYPE,DEVICE", "device"])
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                parts = line.split(":")
                if len(parts) >= 2 and parts[0] == "wifi":
                    return parts[1]
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    return None


def scan_wifi() -> list[WifiNetwork]:
    """Scan for available Wi-Fi networks. Returns deduplicated list sorted by signal."""
    iface = get_wifi_interface()
    if not iface:
        return []

    # Trigger a fresh scan (best-effort)
    try:
        _run(["nmcli", "device", "wifi", "rescan", "ifname", iface], timeout=15)
    except subprocess.SubprocessError:
        pass

    try:
        result = _run([
            "nmcli", "-t", "-f", "SSID,SIGNAL,SECURITY",
            "device", "wifi", "list", "ifname", iface,
        ])
        if result.returncode != 0:
            return []
    except (subprocess.SubprocessError, FileNotFoundError):
        return []

    seen: dict[str, WifiNetwork] = {}
    for line in result.stdout.strip().splitlines():
        # nmcli -t uses : as separator, but SSIDs can contain :.
        # SIGNAL is numeric, SECURITY is at the end.
        # Parse from the right: last field = security, second-to-last = signal
        parts = line.rsplit(":", 2)
        if len(parts) < 3:
            continue
        ssid = parts[0].replace("\\:", ":")
        if not ssid:
            continue
        try:
            signal = int(parts[1])
        except ValueError:
            signal = 0
        security = parts[2]

        # Keep the strongest signal per SSID
        if ssid not in seen or signal > seen[ssid].signal:
            seen[ssid] = WifiNetwork(ssid=ssid, signal=signal, security=security)

    return sorted(seen.values(), key=lambda n: n.signal, reverse=True)


def is_wifi_connected() -> bool:
    """Check if any Wi-Fi connection is active."""
    try:
        result = _run(["nmcli", "-t", "-f", "TYPE,STATE", "connection", "show", "--active"])
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                if line.startswith("802-11-wireless:"):
                    return True
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    return False


def get_active_ssid() -> str | None:
    """Return the SSID of the currently connected Wi-Fi network, or None."""
    iface = get_wifi_interface()
    if not iface:
        return None
    try:
        result = _run([
            "nmcli", "-t", "-f", "GENERAL.CONNECTION",
            "device", "show", iface,
        ])
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                parts = line.split(":", 1)
                if len(parts) == 2 and parts[1] and parts[1] != "--":
                    return parts[1]
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    return None


def connect_wifi(ssid: str, password: str) -> tuple[bool, str]:
    """Connect to a Wi-Fi network. Returns (success, message)."""
    iface = get_wifi_interface()
    if not iface:
        return False, "No Wi-Fi interface found"

    con_name = f"wifi-{ssid}"

    # Delete any existing connection profile for this SSID to avoid conflicts
    try:
        _run(["nmcli", "connection", "delete", con_name], timeout=10)
    except subprocess.SubprocessError:
        pass

    # Create a persistent connection profile with explicit security settings
    try:
        add_cmd = [
            "nmcli", "connection", "add",
            "type", "wifi",
            "con-name", con_name,
            "ifname", iface,
            "ssid", ssid,
            "wifi-sec.key-mgmt", "wpa-psk",
            "wifi-sec.psk", password,
            "connection.autoconnect", "yes",
            "connection.autoconnect-priority", "10",
        ]
        result = _run(add_cmd, timeout=15)
        if result.returncode != 0:
            return False, result.stderr.strip() or "Failed to create connection"

        # Activate the connection
        result = _run([
            "nmcli", "connection", "up", con_name,
        ], timeout=30)

        if result.returncode == 0:
            return True, "Connected successfully"
        else:
            stderr = result.stderr.strip()
            # Clean up failed connection
            try:
                _run(["nmcli", "connection", "delete", con_name], timeout=10)
            except subprocess.SubprocessError:
                pass
            if "Secrets were required" in stderr or "No suitable" in stderr:
                return False, "Incorrect password"
            if "No network with SSID" in stderr:
                return False, f"Network '{ssid}' not found"
            return False, stderr or "Connection failed"
    except subprocess.TimeoutExpired:
        return False, "Connection timed out"
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        return False, str(e)


def start_ap(ssid: str, password: str | None = None) -> bool:
    """Start a Wi-Fi access point using NetworkManager hotspot mode."""
    iface = get_wifi_interface()
    if not iface:
        logger.error("No Wi-Fi interface found for AP mode")
        return False

    # Stop any existing hotspot
    stop_ap()

    if password:
        # WPA-protected hotspot via nmcli shortcut
        cmd = [
            "nmcli", "device", "wifi", "hotspot",
            "ifname", iface,
            "ssid", ssid,
            "band", "bg",
            "channel", "6",
            "password", password,
        ]
    else:
        # Open (no password) AP — must use connection add since nmcli hotspot
        # auto-generates a WPA password when none is specified
        cmd = [
            "nmcli", "connection", "add",
            "type", "wifi",
            "ifname", iface,
            "con-name", "Hotspot",
            "autoconnect", "no",
            "ssid", ssid,
            "wifi.band", "bg",
            "wifi.channel", "6",
            "wifi.mode", "ap",
            "ipv4.method", "shared",
            "ipv6.method", "disabled",
        ]

    try:
        result = _run(cmd, timeout=15)
        if result.returncode != 0:
            logger.error("Failed to start AP: %s", result.stderr.strip())
            return False

        # If we added a connection (open AP), we still need to bring it up
        if not password:
            up_result = _run(
                ["nmcli", "connection", "up", "Hotspot"], timeout=15,
            )
            if up_result.returncode != 0:
                logger.error("Failed to activate AP: %s", up_result.stderr.strip())
                return False

        logger.info("AP started: %s (open=%s)", ssid, not password)
        return True
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        logger.error("Failed to start AP: %s", e)
        return False


def stop_ap() -> None:
    """Stop the Wi-Fi hotspot if active."""
    try:
        # Find and delete hotspot connection
        result = _run(["nmcli", "-t", "-f", "NAME,TYPE", "connection", "show"])
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                parts = line.split(":")
                if len(parts) >= 2 and "Hotspot" in parts[0]:
                    _run(["nmcli", "connection", "delete", parts[0]], timeout=10)
    except (subprocess.SubprocessError, FileNotFoundError):
        pass


def forget_all_wifi() -> None:
    """Delete all saved Wi-Fi connection profiles (for factory reset)."""
    try:
        result = _run(["nmcli", "-t", "-f", "NAME,TYPE", "connection", "show"])
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                parts = line.split(":")
                if len(parts) >= 2 and parts[1] == "802-11-wireless":
                    _run(["nmcli", "connection", "delete", parts[0]], timeout=10)
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
