#!/bin/bash
# Configure systemd-timesyncd to use the CMS server as NTP source.
# Reads cms_host from /opt/agora/persist/cms_config.json.
# Falls back to agora-cms.local if no config exists.
set -e

CMS_CONFIG="/opt/agora/persist/cms_config.json"
TIMESYNCD_CONF="/etc/systemd/timesyncd.conf.d/agora.conf"
FALLBACK="agora-cms.local"

# Extract cms_host from JSON config (no jq dependency — use python)
if [ -f "$CMS_CONFIG" ]; then
    CMS_HOST=$(python3 -c "import json; print(json.load(open('$CMS_CONFIG')).get('cms_host', '$FALLBACK'))" 2>/dev/null || echo "$FALLBACK")
else
    CMS_HOST="$FALLBACK"
fi

# Only rewrite if the NTP server changed
CURRENT=""
if [ -f "$TIMESYNCD_CONF" ]; then
    CURRENT=$(grep -oP '^NTP=\K.*' "$TIMESYNCD_CONF" 2>/dev/null || true)
fi

if [ "$CURRENT" = "$CMS_HOST" ]; then
    exit 0
fi

mkdir -p /etc/systemd/timesyncd.conf.d
cat > "$TIMESYNCD_CONF" <<EOF
[Time]
NTP=$CMS_HOST
FallbackNTP=0.debian.pool.ntp.org 1.debian.pool.ntp.org 2.debian.pool.ntp.org 3.debian.pool.ntp.org
EOF

systemctl restart systemd-timesyncd 2>/dev/null || true
echo "NTP configured to use $CMS_HOST"
