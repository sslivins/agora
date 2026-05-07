#!/bin/bash -e
# pi-gen stage: Install Agora from APT repository and configure for captive portal boot.
#
# Build-time variables (set in pi-gen config):
#   AGORA_BOARD        — zero2w, pi4, pi5 (default: zero2w)
#
# This stage is fully tenant-agnostic. All deployment-specific
# configuration (CMS URL, fleet credentials, WiFi creds) is supplied
# at first boot by /opt/agora/src/scripts/agora-fleet-provision.sh,
# which consumes /boot/firmware/agora-fleet.env stamped by the CMS
# imager. See scripts/agora-fleet-provision.sh for the recognized keys.

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
# The agora-fleet-provision script will write this flag at first boot
# if a CMS URL is supplied via the boot drop-in.
rm -f /opt/agora/persist/provisioned

# ── Disable Pi OS first-boot wizard (user already configured by pi-gen) ──
systemctl disable userconfig 2>/dev/null || true
rm -f /etc/xdg/autostart/piwiz.desktop 2>/dev/null || true

# ── Enable SSH (disabled by default on Pi OS) ──
systemctl enable ssh

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

# ── Configure NTP with public pools (Pi has no battery-backed RTC) ──
mkdir -p /etc/systemd/timesyncd.conf.d
cat > /etc/systemd/timesyncd.conf.d/agora.conf <<'NTP_EOF'
[Time]
NTP=0.debian.pool.ntp.org 1.debian.pool.ntp.org 2.debian.pool.ntp.org 3.debian.pool.ntp.org
NTP_EOF
systemctl enable systemd-timesyncd

# ── Clean up ──
apt-get clean
rm -rf /var/lib/apt/lists/*

CHEOF

# ── Build-time configuration (runs outside chroot, writes into rootfs) ──
# These use pi-gen env vars which aren't available inside the quoted heredoc.

BOARD="${AGORA_BOARD:-zero2w}"
echo "Agora: configuring for board=${BOARD}"

# ── Per-board config.txt adjustments ──
case "${BOARD}" in
  pi4)
    cat >> "${ROOTFS_DIR}/boot/firmware/config.txt" <<'PI4CFG'

# Agora: Pi 4 display config
# Current Bookworm firmware handles HDMI hotplug natively on Pi 4;
# hdmi_force_hotplug was historically needed but now just adds noise.
PI4CFG
    ;;
  pi5)
    cat >> "${ROOTFS_DIR}/boot/firmware/config.txt" <<'PI5CFG'

# Agora: Pi 5 display config
# Pi 5 uses RP1 chip for HDMI — KMS handles hotplug natively
PI5CFG
    ;;
esac

# Write board identifier for runtime detection fallback
mkdir -p "${ROOTFS_DIR}/opt/agora/persist"
echo "${BOARD}" > "${ROOTFS_DIR}/opt/agora/persist/board"

# ── Unblock WiFi radio at every boot ──
# Pi OS soft-blocks wifi via rfkill + the NetworkManager state file.
# Always install the unblock; agora-fleet-provision.sh decides at first
# boot whether a wifi profile actually gets installed (based on CMS-supplied
# creds and presence of wifi hardware). On wifi-less hardware this is a
# harmless noop — `rfkill unblock wifi` does nothing when no wifi switch
# exists.
mkdir -p "${ROOTFS_DIR}/var/lib/NetworkManager"
cat > "${ROOTFS_DIR}/var/lib/NetworkManager/NetworkManager.state" <<'NMSTATE'
[main]
NetworkingEnabled=true
WirelessEnabled=true
WWANEnabled=true
NMSTATE

cat > "${ROOTFS_DIR}/etc/systemd/system/rfkill-unblock-wifi.service" <<'RFKSVC'
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
on_chroot <<'EOF'
systemctl enable rfkill-unblock-wifi
rm -f /var/lib/systemd/rfkill/*
EOF
