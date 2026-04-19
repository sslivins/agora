#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Smoke-test a built Agora .deb by extracting it, installing the
# runtime requirements into a throwaway venv, and importing every
# top-level Python module plus the four systemd entry points.
#
# This exists to catch packaging bugs like the v1.11.2 outage where
# a new top-level package (`hardware/`) was not listed in the
# build-deb.sh allowlist and was omitted from the .deb, crash-looping
# agora-player on every device after upgrade.
#
# Usage: bash packaging/smoke-test-deb.sh <path-to-deb>
# ──────────────────────────────────────────────────────────────
set -euo pipefail

DEB="${1:?Usage: smoke-test-deb.sh <path-to-deb>}"
if [[ ! -f "$DEB" ]]; then
    echo "ERROR: .deb not found at $DEB" >&2
    exit 2
fi

STAGE="$(mktemp -d)"
VENV="$(mktemp -d)/venv"
trap 'rm -rf "$STAGE" "$(dirname "$VENV")"' EXIT

echo "=== Extracting $(basename "$DEB") ==="
dpkg-deb -x "$DEB" "$STAGE"
dpkg-deb -e "$DEB" "$STAGE/DEBIAN"

SRC="$STAGE/opt/agora/src"

echo "=== Verifying structure ==="
REQUIRED_FILES=(
    "$SRC/player/main.py"
    "$SRC/api/main.py"
    "$SRC/cms_client/main.py"
    "$SRC/provision/main.py"
    "$SRC/requirements-player.txt"
    "$SRC/requirements-api.txt"
    "$SRC/requirements-cms-client.txt"
    "$STAGE/etc/systemd/system/agora-player.service"
    "$STAGE/etc/systemd/system/agora-api.service"
    "$STAGE/etc/systemd/system/agora-cms-client.service"
    "$STAGE/etc/systemd/system/agora-provision.service"
    "$STAGE/DEBIAN/control"
    "$STAGE/DEBIAN/postinst"
)
for f in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "$f" ]]; then
        echo "FAIL: missing $f"
        exit 1
    fi
done
echo "  all ${#REQUIRED_FILES[@]} required paths present"

echo "=== Verifying systemd unit ExecStart paths resolve inside package ==="
for unit in "$STAGE"/etc/systemd/system/agora-*.service; do
    script=$(awk -F'=' '/^ExecStart=/{
        # ExecStart=/usr/bin/python3 /opt/agora/src/player/main.py
        split($2, parts, " ")
        print parts[2]
        exit
    }' "$unit")
    if [[ -z "$script" ]]; then
        echo "FAIL: $(basename "$unit") has no ExecStart"
        exit 1
    fi
    rel="${script#/}"
    if [[ ! -f "$STAGE/$rel" ]]; then
        echo "FAIL: $(basename "$unit") ExecStart=$script but $STAGE/$rel missing"
        exit 1
    fi
    echo "  $(basename "$unit") → $script ✓"
done

echo "=== Byte-compiling all shipped Python sources ==="
python3 -m compileall -q "$SRC"

echo "=== Creating venv + installing runtime requirements ==="
python3 -m venv "$VENV"
# shellcheck disable=SC1091
. "$VENV/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet \
    -r "$SRC/requirements-api.txt" \
    -r "$SRC/requirements-player.txt" \
    -r "$SRC/requirements-cms-client.txt"

echo "=== Import smoke: every top-level package ==="
export PYTHONPATH="$SRC"
# Every top-level package with __init__.py — auto-discovered so new
# packages are exercised without touching this script.
mapfile -t PACKAGES < <(
    for init in "$SRC"/*/__init__.py; do
        [[ -f "$init" ]] && basename "$(dirname "$init")"
    done
)
if [[ ${#PACKAGES[@]} -eq 0 ]]; then
    echo "FAIL: no Python packages found under $SRC"
    exit 1
fi
for pkg in "${PACKAGES[@]}"; do
    python3 -c "import importlib; importlib.import_module('$pkg'); print('ok: $pkg')"
done

echo "=== Import smoke: systemd entry points ==="
# These four imports match the four systemd units' ExecStart targets.
# If any top-level import chain is broken (like the v1.11.2 missing
# hardware/ module), one of these will fail the same way the Pi did.
for entry in player.main api.main cms_client.main provision.main; do
    python3 -c "import importlib; importlib.import_module('$entry'); print('ok: $entry')"
done

echo ""
echo "=== SMOKE PASSED ==="
