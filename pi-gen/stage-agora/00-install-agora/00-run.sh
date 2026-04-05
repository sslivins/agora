#!/bin/bash -e
# pi-gen stage: Install Agora from APT repository and configure for captive portal boot.

on_chroot <<'CHEOF'

# ── Add Agora apt repository ──
REPO_URL="https://sslivins.github.io/agora"
echo "deb [arch=arm64 trusted=yes] ${REPO_URL} stable main" > /etc/apt/sources.list.d/agora.list
apt-get update -qq

# ── Install Agora (pulls in network-manager, dnsmasq, avahi-daemon) ──
apt-get install -y agora

# ── Disable cloud-init (not needed on embedded Pi, saves ~6s boot time) ──
touch /etc/cloud/cloud-init.disabled

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

# ── Fix HDMI display output for KMS driver ──
# disable_fw_kms_setup=1 (pi-gen default) prevents firmware from passing display
# mode info to the vc4-kms-v3d kernel driver, causing kmssink to fail.
sed -i 's/^disable_fw_kms_setup=1/disable_fw_kms_setup=0/' /boot/firmware/config.txt 2>/dev/null || true
# Redirect console=tty1 to tty3 — keeps Plymouth on tty1 while hiding
# kernel/systemd messages on an off-screen TTY
sed -i 's/console=tty1/console=tty3/g' /boot/firmware/cmdline.txt 2>/dev/null || true
# Force HDMI connector detection with 1080p mode on kernel cmdline
sed -i 's/rootwait/rootwait video=HDMI-A-1:1920x1080@60D/' /boot/firmware/cmdline.txt 2>/dev/null || true

# ── Clean up ──
apt-get clean
rm -rf /var/lib/apt/lists/*

CHEOF
