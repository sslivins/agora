#!/bin/bash
# Safe OOBE launcher — runs pre-flight check, then starts OOBE with --force-oobe.
# Logs are copied to the boot partition (FAT32) for Windows-readable crash diagnosis.
#
# Usage:
#   sudo bash /opt/agora/src/provision/launch_oobe.sh
#
# This does NOT delete Wi-Fi or the provisioned flag — --force-oobe bypasses
# the provisioned check, and AP mode takes over the Wi-Fi interface automatically.

set -e

LOG=/tmp/oobe.log
BOOT_LOG=/boot/firmware/oobe-crash.log

# Clear old logs
rm -f "$LOG" "$BOOT_LOG"

echo "$(date): OOBE launcher starting" > "$LOG"

# Pre-flight: verify imports work before doing anything destructive
echo "$(date): Running import check..." >> "$LOG"
if ! PYTHONPATH=/opt/agora/src python3 -c 'from provision import display, service, app, network; import qrcode' >> "$LOG" 2>&1; then
    echo "$(date): IMPORT CHECK FAILED — aborting (Wi-Fi preserved)" >> "$LOG"
    cp "$LOG" "$BOOT_LOG"
    echo "IMPORT CHECK FAILED — see $LOG"
    exit 1
fi
echo "$(date): Import check passed" >> "$LOG"

# Stop services that hold KMS/DRM (player) or bind ports (api, provision)
echo "$(date): Stopping conflicting services..." >> "$LOG"
systemctl stop agora-player agora-api agora-cms-client agora-provision 2>/dev/null || true

# Launch the OOBE service with --force-oobe (no need to delete provisioned flag)
echo "$(date): Launching OOBE service..." >> "$LOG"
cd /opt/agora/src
PYTHONPATH=/opt/agora/src python3 -u provision/service.py --force-oobe >> "$LOG" 2>&1
EXIT_CODE=$?

echo "$(date): OOBE exited with code $EXIT_CODE" >> "$LOG"

# Copy log to boot partition so it's readable from Windows
cp "$LOG" "$BOOT_LOG" 2>/dev/null || true

exit $EXIT_CODE
