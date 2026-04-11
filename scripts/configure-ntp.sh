#!/bin/bash
# Configure systemd-timesyncd to use public NTP pools.
# Removes any previous CMS-based NTP configuration.
set -e

TIMESYNCD_CONF="/etc/systemd/timesyncd.conf.d/agora.conf"
NTP_SERVERS="0.debian.pool.ntp.org 1.debian.pool.ntp.org 2.debian.pool.ntp.org 3.debian.pool.ntp.org"

# Only rewrite if the config doesn't match
if [ -f "$TIMESYNCD_CONF" ]; then
    CURRENT=$(grep -oP '^NTP=\K.*' "$TIMESYNCD_CONF" 2>/dev/null || true)
    if [ "$CURRENT" = "$NTP_SERVERS" ]; then
        exit 0
    fi
fi

mkdir -p /etc/systemd/timesyncd.conf.d
cat > "$TIMESYNCD_CONF" <<EOF
[Time]
NTP=$NTP_SERVERS
EOF

systemctl restart systemd-timesyncd 2>/dev/null || true
echo "NTP configured to use public pools"
