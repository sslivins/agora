"""Tests for provision.network — Ethernet and WiFi detection helpers."""

from unittest.mock import MagicMock, patch
import subprocess

import pytest

from provision import network


# ── get_ethernet_interface ───────────────────────────────────────────────────


class TestGetEthernetInterface:
    """Tests for get_ethernet_interface()."""

    @patch("provision.network._run")
    def test_returns_first_ethernet_device(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="ethernet:eth0\nwifi:wlan0\n",
        )
        assert network.get_ethernet_interface() == "eth0"

    @patch("provision.network._run")
    def test_returns_none_when_no_ethernet(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="wifi:wlan0\n",
        )
        assert network.get_ethernet_interface() is None

    @patch("provision.network._run")
    def test_returns_none_on_nmcli_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        assert network.get_ethernet_interface() is None

    @patch("provision.network._run")
    def test_returns_none_on_exception(self, mock_run):
        mock_run.side_effect = FileNotFoundError("nmcli not found")
        assert network.get_ethernet_interface() is None

    @patch("provision.network._run")
    def test_multiple_ethernet_returns_first(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="ethernet:eth0\nethernet:eth1\n",
        )
        assert network.get_ethernet_interface() == "eth0"


# ── is_ethernet_connected ───────────────────────────────────────────────────


class TestIsEthernetConnected:
    """Tests for is_ethernet_connected()."""

    @patch("provision.network._run")
    @patch("provision.network.get_ethernet_interface", return_value="eth0")
    def test_connected_with_ip(self, mock_iface, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="IP4.ADDRESS[1]:192.168.1.100/24\n",
        )
        assert network.is_ethernet_connected() is True

    @patch("provision.network.get_ethernet_interface", return_value=None)
    def test_no_interface(self, mock_iface):
        assert network.is_ethernet_connected() is False

    @patch("provision.network._run")
    @patch("provision.network.get_ethernet_interface", return_value="eth0")
    def test_no_ip(self, mock_iface, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="IP4.ADDRESS[1]:\n",
        )
        assert network.is_ethernet_connected() is False

    @patch("provision.network._run")
    @patch("provision.network.get_ethernet_interface", return_value="eth0")
    def test_nmcli_failure(self, mock_iface, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        assert network.is_ethernet_connected() is False


# ── get_device_ip (multi-interface) ──────────────────────────────────────────


class TestGetDeviceIp:
    """Tests for get_device_ip() — now checks WiFi and Ethernet."""

    @patch("provision.network._run")
    @patch("provision.network.get_ethernet_interface", return_value=None)
    @patch("provision.network.get_wifi_interface", return_value="wlan0")
    def test_returns_wifi_ip(self, mock_wifi, mock_eth, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="IP4.ADDRESS[1]:10.0.0.5/24\n",
        )
        assert network.get_device_ip() == "10.0.0.5"

    @patch("provision.network._run")
    @patch("provision.network.get_ethernet_interface", return_value="eth0")
    @patch("provision.network.get_wifi_interface", return_value=None)
    def test_returns_ethernet_ip_when_no_wifi(self, mock_wifi, mock_eth, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="IP4.ADDRESS[1]:192.168.1.50/24\n",
        )
        assert network.get_device_ip() == "192.168.1.50"

    @patch("provision.network.get_ethernet_interface", return_value=None)
    @patch("provision.network.get_wifi_interface", return_value=None)
    def test_returns_none_when_no_interfaces(self, mock_wifi, mock_eth):
        assert network.get_device_ip() is None

    @patch("provision.network._run")
    @patch("provision.network.get_ethernet_interface", return_value="eth0")
    @patch("provision.network.get_wifi_interface", return_value="wlan0")
    def test_prefers_wifi_over_ethernet(self, mock_wifi, mock_eth, mock_run):
        """WiFi is checked first; if it has an IP, ethernet isn't checked."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="IP4.ADDRESS[1]:10.0.0.5/24\n",
        )
        assert network.get_device_ip() == "10.0.0.5"
        # Only called once for WiFi interface
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "wlan0" in args

    @patch("provision.network._run")
    @patch("provision.network.get_ethernet_interface", return_value="eth0")
    @patch("provision.network.get_wifi_interface", return_value="wlan0")
    def test_falls_back_to_ethernet_when_wifi_has_no_ip(self, mock_wifi, mock_eth, mock_run):
        """If WiFi has no IP address, fall back to ethernet."""
        wifi_result = MagicMock(returncode=0, stdout="")
        eth_result = MagicMock(
            returncode=0,
            stdout="IP4.ADDRESS[1]:192.168.1.50/24\n",
        )
        mock_run.side_effect = [wifi_result, eth_result]
        assert network.get_device_ip() == "192.168.1.50"
        assert mock_run.call_count == 2
