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

# ── Unblock WiFi radio (Pi OS soft-blocks it via rfkill + NM state file) ──
# 1. Write NM state file with WiFi enabled (NM honors this over rfkill)
mkdir -p /var/lib/NetworkManager
cat > /var/lib/NetworkManager/NetworkManager.state <<'NMSTATE'
[main]
NetworkingEnabled=true
WirelessEnabled=true
WWANEnabled=true
NMSTATE

# 2. Create a service that unblocks rfkill AFTER /dev/rfkill exists but BEFORE NM
cat > /etc/systemd/system/rfkill-unblock-wifi.service <<'RFKSVC'
[Unit]
Description=Unblock WiFi radio
After=systemd-udevd.service systemd-rfkill.service
Before=NetworkManager.service
Wants=systemd-udevd.service

[Service]
Type=oneshot
ExecStartPre=/bin/sh -c 'for i in $(seq 1 30); do [ -e /dev/rfkill ] && exit 0; sleep 0.5; done; exit 0'
ExecStart=/usr/sbin/rfkill unblock wifi
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
RFKSVC
systemctl enable rfkill-unblock-wifi

# 3. Delete systemd-rfkill saved state so it doesn't restore the block on boot
rm -f /var/lib/systemd/rfkill/*

mkdir -p /etc/NetworkManager/system-connections

# ── Clean up ──
apt-get clean
rm -rf /var/lib/apt/lists/*

CHEOF
