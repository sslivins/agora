"""DNS redirect configuration for captive portal.

When in AP mode, dnsmasq intercepts all DNS queries and redirects
them to the device's IP, triggering captive portal detection on
connected clients.
"""

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger("agora.provision.dns")

DNSMASQ_CONF_PATH = Path("/etc/dnsmasq.d/agora-portal.conf")
AP_GATEWAY = "10.42.0.1"  # NetworkManager hotspot default gateway

DNSMASQ_CONF = f"""\
# Agora captive portal DNS redirect
# Managed by agora-provision — do not edit manually
interface=wlan0
bind-interfaces
address=/#/{AP_GATEWAY}
"""


def install_dns_redirect() -> bool:
    """Write dnsmasq config that redirects all DNS to the AP gateway."""
    try:
        DNSMASQ_CONF_PATH.parent.mkdir(parents=True, exist_ok=True)
        DNSMASQ_CONF_PATH.write_text(DNSMASQ_CONF)
        _restart_dnsmasq()
        logger.info("DNS redirect installed")
        return True
    except OSError as e:
        logger.error("Failed to install DNS redirect: %s", e)
        return False


def remove_dns_redirect() -> bool:
    """Remove the captive portal DNS config and restart dnsmasq."""
    try:
        if DNSMASQ_CONF_PATH.exists():
            DNSMASQ_CONF_PATH.unlink()
        _restart_dnsmasq()
        logger.info("DNS redirect removed")
        return True
    except OSError as e:
        logger.error("Failed to remove DNS redirect: %s", e)
        return False


def _restart_dnsmasq() -> None:
    """Restart or stop dnsmasq service."""
    try:
        if DNSMASQ_CONF_PATH.exists():
            subprocess.run(
                ["systemctl", "restart", "dnsmasq"],
                capture_output=True, timeout=10,
            )
        else:
            subprocess.run(
                ["systemctl", "stop", "dnsmasq"],
                capture_output=True, timeout=10,
            )
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
