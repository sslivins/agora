"""Tests for is_wifi_connected() — must verify actual IP connectivity.

Reproduces issue #64: NM can report a Wi-Fi connection as "active" before DHCP
completes, causing is_wifi_connected() to return True when the device has no IP.
"""

from unittest.mock import MagicMock, patch

from provision.network import is_wifi_connected


def _mock_run(nmcli_output: str, ip_output: str | None = None):
    """Return a side_effect function that returns different output for
    the active-connection check vs the IP address check."""
    call_count = 0

    def side_effect(args, **kwargs):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        result.returncode = 0

        # First call: nmcli connection show --active
        if "connection" in args and "--active" in args:
            result.stdout = nmcli_output
            return result

        # IP address check (get_device_ip path): nmcli device show
        if "device" in args and "show" in args:
            # get_wifi_interface call
            if "TYPE,DEVICE" in args or ("TYPE" in str(args) and "DEVICE" in str(args)):
                result.stdout = "wifi:wlan0\n"
                return result
            # IP4.ADDRESS call
            result.stdout = ip_output or ""
            return result

        # get_wifi_interface fallback
        if "-f" in args:
            idx = args.index("-f") + 1
            if idx < len(args) and "TYPE" in args[idx]:
                result.stdout = "wifi:wlan0\n"
                return result

        result.stdout = ""
        return result

    return side_effect


class TestIsWifiConnected:
    """is_wifi_connected() must verify the device has an actual IP address."""

    def test_active_wifi_with_ip_returns_true(self):
        """Active Wi-Fi connection + valid IP → connected."""
        with patch("provision.network._run", side_effect=_mock_run(
            nmcli_output="802-11-wireless:activated\n",
            ip_output="IP4.ADDRESS[1]:192.168.1.50/24\n",
        )):
            assert is_wifi_connected() is True

    def test_active_wifi_without_ip_returns_false(self):
        """Active Wi-Fi connection but no IP (DHCP not done) → NOT connected.

        This is the core bug from issue #64: NM shows the connection as
        'activated' before the device has an IP address.
        """
        with patch("provision.network._run", side_effect=_mock_run(
            nmcli_output="802-11-wireless:activated\n",
            ip_output="",
        )):
            assert is_wifi_connected() is False

    def test_no_active_wifi_returns_false(self):
        """No active Wi-Fi connection → not connected (unchanged behavior)."""
        with patch("provision.network._run", side_effect=_mock_run(
            nmcli_output="",
        )):
            assert is_wifi_connected() is False

    def test_nmcli_failure_returns_false(self):
        """nmcli failure → not connected (unchanged behavior)."""
        def fail(*args, **kwargs):
            result = MagicMock()
            result.returncode = 1
            result.stdout = ""
            return result

        with patch("provision.network._run", side_effect=fail):
            assert is_wifi_connected() is False
