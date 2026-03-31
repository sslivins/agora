"""DNS redirect configuration for captive portal.

When in AP mode, NetworkManager's shared connection spawns its own dnsmasq
instance. We drop a config file into NM's dnsmasq-shared.d/ directory to
redirect all DNS queries to the AP gateway, triggering captive portal
detection on connected clients.
"""

import logging
from pathlib import Path

logger = logging.getLogger("agora.provision.dns")

# NM's shared-mode dnsmasq reads extra config from this directory
NM_DNSMASQ_DIR = Path("/etc/NetworkManager/dnsmasq-shared.d")
PORTAL_CONF = NM_DNSMASQ_DIR / "agora-portal.conf"
AP_GATEWAY = "10.42.0.1"  # NetworkManager hotspot default gateway

DNSMASQ_CONF = f"""\
# Agora captive portal DNS redirect
# Managed by agora-provision — do not edit manually
address=/#/{AP_GATEWAY}
"""


def install_dns_redirect() -> bool:
    """Write dnsmasq config that redirects all DNS to the AP gateway.

    Must be called BEFORE starting the AP so NM's dnsmasq picks it up.
    """
    try:
        NM_DNSMASQ_DIR.mkdir(parents=True, exist_ok=True)
        PORTAL_CONF.write_text(DNSMASQ_CONF)
        logger.info("DNS redirect installed")
        return True
    except OSError as e:
        logger.error("Failed to install DNS redirect: %s", e)
        return False


def remove_dns_redirect() -> bool:
    """Remove the captive portal DNS config."""
    try:
        if PORTAL_CONF.exists():
            PORTAL_CONF.unlink()
        logger.info("DNS redirect removed")
        return True
    except OSError as e:
        logger.error("Failed to remove DNS redirect: %s", e)
        return False
