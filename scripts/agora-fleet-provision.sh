#!/bin/bash
# ──────────────────────────────────────────────────────────────────────
# agora-fleet-provision.sh
#
# First-boot fleet provisioning for bootstrap v2.
#
# Run as root by agora-fleet-provision.service, before agora-cms-client.
# Idempotent: safe to re-run on every boot.
#
# Responsibilities:
#   1. Ensure /etc/agora/environment exists with AGORA_BOOTSTRAP_V2=1 default.
#   2. If /boot/firmware/agora-fleet.env (or /boot/agora-fleet.env on older
#      Pi OS) is present, install its AGORA_FLEET_ID / AGORA_FLEET_SECRET_HEX
#      values into /etc/agora/environment (0600 root), then shred the source
#      so a lost SD card cannot leak the fleet secret.
#   3. Ensure /opt/agora/persist is owned by root:root (bootstrap v2 files
#      require it; other persist files are mode 0644 and remain readable
#      by agora-api / agora-player running as user agora).
#
# Operators wanting to re-provision a device with a different fleet id
# can drop a fresh agora-fleet.env on the boot partition and reboot.
# ──────────────────────────────────────────────────────────────────────
set -euo pipefail

ENV_FILE="/etc/agora/environment"
BOOT_FLEET_FILE="/boot/firmware/agora-fleet.env"
LEGACY_BOOT_FLEET_FILE="/boot/agora-fleet.env"
PERSIST_DIR="/opt/agora/persist"

mkdir -p /etc/agora
chmod 0755 /etc/agora 2>/dev/null || true
chown root:root /etc/agora 2>/dev/null || true

# ── Seed env file with bootstrap v2 default ──
if [ ! -f "$ENV_FILE" ]; then
    touch "$ENV_FILE"
fi
chmod 0600 "$ENV_FILE" 2>/dev/null || true
chown root:root "$ENV_FILE" 2>/dev/null || true

if ! grep -q '^AGORA_BOOTSTRAP_V2=' "$ENV_FILE"; then
    echo "AGORA_BOOTSTRAP_V2=1" >> "$ENV_FILE"
    echo "agora-fleet-provision: enabled bootstrap v2 default"
fi

# ── Pick up fleet config drop-in from boot partition ──
src=""
if [ -f "$BOOT_FLEET_FILE" ]; then
    src="$BOOT_FLEET_FILE"
elif [ -f "$LEGACY_BOOT_FLEET_FILE" ]; then
    src="$LEGACY_BOOT_FLEET_FILE"
fi

if [ -n "$src" ]; then
    echo "agora-fleet-provision: installing fleet config from $src"
    # Allow-listed keys only; ignore anything else in the drop-in to
    # keep the attack surface tight (no arbitrary env vars).
    while IFS='=' read -r key val; do
        # skip blanks and comments
        case "$key" in
            ''|\#*) continue ;;
        esac
        # strip Windows CRLF if present
        val="${val%$'\r'}"
        # strip surrounding quotes
        val="${val%\"}"
        val="${val#\"}"
        case "$key" in
            AGORA_FLEET_ID|AGORA_FLEET_SECRET_HEX|AGORA_BOOTSTRAP_V2)
                sed -i "/^${key}=/d" "$ENV_FILE"
                echo "${key}=${val}" >> "$ENV_FILE"
                echo "agora-fleet-provision: set ${key}"
                ;;
        esac
    done < "$src"
    chmod 0600 "$ENV_FILE" 2>/dev/null || true
    chown root:root "$ENV_FILE" 2>/dev/null || true
    # Shred the boot-partition copy so a stolen SD card doesn't leak
    # the fleet secret. shred is a no-op on FAT (no overwrite possible),
    # but it still unlinks the file.
    if command -v shred >/dev/null 2>&1; then
        shred -u "$src" 2>/dev/null || rm -f "$src"
    else
        rm -f "$src"
    fi
    echo "agora-fleet-provision: fleet config installed; boot drop-in removed"
fi

# ── Ensure persist dir is root-owned (bootstrap v2 requirement) ──
# Mode 0755 lets agora-api / agora-player (user agora) read files like
# api_key (which atomic_write creates 0644). Bootstrap v2 files are
# mode 0600 root:root and remain private.
if [ -d "$PERSIST_DIR" ]; then
    chown -R root:root "$PERSIST_DIR" 2>/dev/null || true
    chmod 0755 "$PERSIST_DIR" 2>/dev/null || true
fi

exit 0
