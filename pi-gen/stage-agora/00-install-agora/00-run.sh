#!/bin/bash -e
# pi-gen stage: Install Agora from APT repository and configure for captive portal boot.

on_chroot <<'CHEOF'

# ── Add Agora apt repository ──
REPO_URL="https://sslivins.github.io/agora"
echo "deb [arch=arm64 trusted=yes] ${REPO_URL} stable main" > /etc/apt/sources.list.d/agora.list
apt-get update -qq

# ── Install Agora (pulls in network-manager, dnsmasq, avahi-daemon) ──
apt-get install -y agora

# ── Ensure device boots into captive portal (no provisioned flag) ──
rm -f /opt/agora/persist/provisioned

# ── Disable Pi OS first-boot wizard (user already configured by pi-gen) ──
systemctl disable userconfig 2>/dev/null || true
rm -f /etc/xdg/autostart/piwiz.desktop 2>/dev/null || true

# ── Enable SSH (disabled by default on Pi OS) ──
systemctl enable ssh

# ── DEBUG: Disable player so console stays visible on HDMI ──
systemctl disable agora-player 2>/dev/null || true

# ── DEBUG: Show boot messages on console (remove quiet/splash) ──
sed -i 's/ quiet//g; s/ splash//g' /boot/firmware/cmdline.txt 2>/dev/null || true

# ── DEBUG: USB gadget Ethernet — SSH over USB cable, no dongle needed ──
# Add dwc2 overlay to config.txt
if ! grep -q 'dtoverlay=dwc2' /boot/firmware/config.txt; then
  echo 'dtoverlay=dwc2' >> /boot/firmware/config.txt
fi
# Load modules via kernel cmdline (more reliable than /etc/modules)
sed -i 's/$/ modules-load=dwc2,g_ether/' /boot/firmware/cmdline.txt
# Configure static IP on usb0 so we know where to SSH
mkdir -p /etc/NetworkManager/system-connections
cat > /etc/NetworkManager/system-connections/usb0-static.nmconnection <<'NMEOF'
[connection]
id=usb0-static
type=ethernet
interface-name=usb0
autoconnect=true

[ipv4]
method=manual
addresses=10.42.0.2/24

[ipv6]
method=disabled
NMEOF
chmod 600 /etc/NetworkManager/system-connections/usb0-static.nmconnection

# ── DEBUG: Ensure console login on HDMI ──
systemctl enable getty@tty1 2>/dev/null || true

# ── Clean up ──
apt-get clean
rm -rf /var/lib/apt/lists/*

CHEOF
