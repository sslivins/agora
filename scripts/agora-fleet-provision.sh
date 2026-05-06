#!/bin/bash
# ──────────────────────────────────────────────────────────────────────
# agora-fleet-provision.sh
#
# First-boot fleet provisioning for bootstrap v2.
#
# Run as root by agora-fleet-provision.service, before agora-cms-client.
# Idempotent: safe to re-run on every boot.
#
# This script is the SINGLE entry point for deployment-specific
# configuration. The pi-gen catalog image is fully tenant-agnostic;
# everything that used to be baked in at build time now arrives via
# the CMS imager's boot drop-in (/boot/firmware/agora-fleet.env).
#
# Recognized keys in the boot drop-in:
#
#   /etc/agora/environment (mode 0600 root:root)
#       AGORA_FLEET_ID              fleet identifier
#       AGORA_FLEET_SECRET_HEX      fleet shared secret (hex)
#       AGORA_BOOTSTRAP_V2          bootstrap protocol toggle (default 1)
#       AGORA_CMS_TRANSPORT         direct | wps
#
#   /opt/agora/persist/cms_config.json (mode 0644 root:root)
#       AGORA_CMS_URL               wss://host[:port]/path
#                                   (host/port parsed from URL)
#       Also writes /opt/agora/persist/provisioned to skip OOBE.
#
#   /etc/NetworkManager/system-connections/wifi-<ssid>.nmconnection
#   (mode 0600 root:root) — only if device has wifi hardware
#       AGORA_WIFI_SSID             wifi network name
#       AGORA_WIFI_PASS             wifi password (PSK)
#                                   blank SSID = "do not use wifi"
#
# Critical write ordering: the boot drop-in is shredded LAST, after
# every persistent write (env file, cms_config.json, provisioned flag,
# NetworkManager profile) has been flushed to disk. If the script
# crashes mid-way, the drop-in remains and the next boot retries
# cleanly. Shredding before the writes would brick the device on
# crash (secret material gone, no fleet config, no recovery short
# of re-flash).
#
# Operators wanting to re-provision a device with a different fleet id
# can drop a fresh agora-fleet.env on the boot partition and reboot.
# ──────────────────────────────────────────────────────────────────────
set -euo pipefail

ENV_FILE="/etc/agora/environment"
BOOT_FLEET_FILE="/boot/firmware/agora-fleet.env"
LEGACY_BOOT_FLEET_FILE="/boot/agora-fleet.env"
PERSIST_DIR="/opt/agora/persist"
CMS_CONFIG_FILE="${PERSIST_DIR}/cms_config.json"
PROVISIONED_FLAG="${PERSIST_DIR}/provisioned"
NM_CONNECTIONS_DIR="/etc/NetworkManager/system-connections"

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

    # Capture out-of-band values into shell vars; allow-listed env-file
    # keys append to $ENV_FILE in the loop. Anything else is silently
    # ignored to keep the attack surface tight.
    captured_cms_url=""
    captured_wifi_ssid=""
    captured_wifi_pass=""

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
            AGORA_FLEET_ID|AGORA_FLEET_SECRET_HEX|AGORA_BOOTSTRAP_V2|AGORA_CMS_TRANSPORT)
                sed -i "/^${key}=/d" "$ENV_FILE"
                echo "${key}=${val}" >> "$ENV_FILE"
                echo "agora-fleet-provision: set ${key}"
                ;;
            AGORA_CMS_URL)
                captured_cms_url="$val"
                ;;
            AGORA_WIFI_SSID)
                captured_wifi_ssid="$val"
                ;;
            AGORA_WIFI_PASS)
                captured_wifi_pass="$val"
                ;;
        esac
    done < "$src"
    chmod 0600 "$ENV_FILE" 2>/dev/null || true
    chown root:root "$ENV_FILE" 2>/dev/null || true
    sync

    # ── CMS URL → cms_config.json + provisioned flag ──
    # The CMS client reads cms_config.json (NOT $ENV_FILE) for host/port,
    # and the provisioned flag bypasses OOBE captive portal so the device
    # boots straight into player mode against the fleet CMS.
    if [ -n "$captured_cms_url" ]; then
        echo "agora-fleet-provision: configuring CMS URL '${captured_cms_url}'"
        mkdir -p "$PERSIST_DIR"
        # Parse host:port from ws://host:port/path or wss://host:port/path.
        # If no explicit port in the URL, default to 8080 (matches pi-gen
        # historical behavior).
        cms_host=$(echo "$captured_cms_url" | sed -E 's|^wss?://([^:/]+).*|\1|')
        cms_port=$(echo "$captured_cms_url" | sed -E 's|^wss?://[^:]+:([0-9]+).*|\1|')
        if [ "$cms_port" = "$captured_cms_url" ]; then
            cms_port="8080"
        fi
        cat > "$CMS_CONFIG_FILE" <<CMSCFG
{
  "cms_host": "${cms_host}",
  "cms_port": ${cms_port},
  "cms_url": "${captured_cms_url}"
}
CMSCFG
        chown root:root "$CMS_CONFIG_FILE" 2>/dev/null || true
        chmod 0644 "$CMS_CONFIG_FILE" 2>/dev/null || true
        sync

        # Mark provisioned so OOBE is skipped on first boot. Without
        # this, the captive portal still starts and steals the wifi
        # interface even though we have the CMS config.
        echo "1" > "$PROVISIONED_FLAG"
        chown root:root "$PROVISIONED_FLAG" 2>/dev/null || true
        chmod 0644 "$PROVISIONED_FLAG" 2>/dev/null || true
        sync
        echo "agora-fleet-provision: marked provisioned"
    fi

    # ── WiFi creds → NetworkManager .nmconnection ──
    # Hardware-detect via sysfs (NOT nmcli — fleet-provision runs
    # before NetworkManager.service, so nmcli would always return
    # nothing on a fresh boot). cfg80211 populates /sys/class/ieee80211
    # during udev coldplug, well before local-fs.target succeeds.
    if [ -n "$captured_wifi_ssid" ]; then
        if ls /sys/class/ieee80211/*/ >/dev/null 2>&1 \
           || [ -d /sys/class/net/wlan0 ]; then
            echo "agora-fleet-provision: configuring WiFi network '${captured_wifi_ssid}'"
            mkdir -p "$NM_CONNECTIONS_DIR"
            con_file="${NM_CONNECTIONS_DIR}/wifi-${captured_wifi_ssid}.nmconnection"
            cat > "$con_file" <<WIFICFG
[connection]
id=wifi-${captured_wifi_ssid}
type=wifi
autoconnect=true
autoconnect-priority=10

[wifi]
ssid=${captured_wifi_ssid}
mode=infrastructure

[wifi-security]
key-mgmt=wpa-psk
psk=${captured_wifi_pass}

[ipv4]
method=auto

[ipv6]
method=auto
WIFICFG
            chown root:root "$con_file" 2>/dev/null || true
            chmod 0600 "$con_file" 2>/dev/null || true
            sync

            # Best-effort live activation. NetworkManager may not be
            # running yet (we're ordered before it for cms-client
            # ordering reasons), in which case it'll pick the file up
            # at its own startup since it lives in the canonical
            # location. On devices that already have a working
            # ethernet link, NM will not switch off it mid-boot —
            # the wifi profile takes effect on next boot or on link
            # loss. Tolerate failure.
            nmcli connection reload 2>/dev/null || true
            nmcli connection up "wifi-${captured_wifi_ssid}" 2>/dev/null || true
        else
            echo "agora-fleet-provision: no wifi hardware detected, skipping WiFi config"
        fi
    fi

    # Shred the boot-partition copy LAST, after every persistent write
    # has been flushed. shred is a no-op on FAT (no overwrite possible),
    # but it still unlinks the file.
    if command -v shred >/dev/null 2>&1; then
        shred -u "$src" 2>/dev/null || rm -f "$src"
    else
        rm -f "$src"
    fi
    sync
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
