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

# ── Unblock WiFi radio (rfkill soft-blocks it by default on Pi OS) ──
rfkill unblock wifi 2>/dev/null || true

# Ensure WiFi is unblocked on every boot via NetworkManager dispatcher
mkdir -p /etc/NetworkManager/dispatcher.d
cat > /etc/NetworkManager/dispatcher.d/pre-up.d/10-unblock-wifi <<'RFKEOF'
#!/bin/bash
rfkill unblock wifi
RFKEOF
chmod +x /etc/NetworkManager/dispatcher.d/pre-up.d/10-unblock-wifi

# Also create a one-shot service that runs before NM to unblock wifi
cat > /etc/systemd/system/rfkill-unblock-wifi.service <<'RFKSVC'
[Unit]
Description=Unblock WiFi radio
DefaultDependencies=no
Before=NetworkManager.service
After=systemd-rfkill.service

[Service]
Type=oneshot
ExecStart=/usr/sbin/rfkill unblock wifi
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
RFKSVC
systemctl enable rfkill-unblock-wifi

# ── DEBUG: USB gadget removed — USB port stays in host mode for Ethernet dongle ──
mkdir -p /etc/NetworkManager/system-connections

# ── DEBUG: WiFi credentials for development SSH access ──
cat > /etc/NetworkManager/system-connections/debug-wifi.nmconnection <<'WIFIEOF'
[connection]
id=debug-wifi
type=wifi
autoconnect=true
autoconnect-priority=100

[wifi]
ssid=y'all 2.4ghz
mode=infrastructure

[wifi-security]
key-mgmt=wpa-psk
psk=bricklebush

[ipv4]
method=auto

[ipv6]
method=auto
WIFIEOF
chmod 600 /etc/NetworkManager/system-connections/debug-wifi.nmconnection

# ── DEBUG: Ensure console login on HDMI ──
systemctl enable getty@tty1 2>/dev/null || true

# ── DEBUG: Dump logs to boot partition (readable from Windows) ──
cat > /usr/local/bin/agora-debug-dump.sh <<'DUMPEOF'
#!/bin/bash
# Wait for boot to settle
sleep 30
LOGDIR=/boot/firmware/debug-logs
mkdir -p "$LOGDIR"
journalctl --no-pager > "$LOGDIR/journal.txt" 2>&1
journalctl -u agora-provision --no-pager > "$LOGDIR/provision.txt" 2>&1
journalctl -u NetworkManager --no-pager > "$LOGDIR/networkmanager.txt" 2>&1
nmcli device > "$LOGDIR/nmcli-device.txt" 2>&1
nmcli connection show > "$LOGDIR/nmcli-connections.txt" 2>&1
ip addr > "$LOGDIR/ip-addr.txt" 2>&1
systemctl list-units --failed > "$LOGDIR/failed-units.txt" 2>&1
dmesg > "$LOGDIR/dmesg.txt" 2>&1
echo "Debug dump complete at $(date)" > "$LOGDIR/done.txt"
DUMPEOF
chmod +x /usr/local/bin/agora-debug-dump.sh

# Create a systemd service for the debug dump
cat > /etc/systemd/system/agora-debug-dump.service <<'SVCEOF'
[Unit]
Description=Agora Debug Log Dump
After=agora-provision.service NetworkManager.service
Wants=agora-provision.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/agora-debug-dump.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
SVCEOF
systemctl enable agora-debug-dump

# ── Clean up ──
apt-get clean
rm -rf /var/lib/apt/lists/*

CHEOF
