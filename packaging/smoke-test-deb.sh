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
    "$STAGE/etc/systemd/system/agora-fleet-provision.service"
    "$SRC/scripts/agora-fleet-provision.sh"
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

echo "=== Verifying systemd unit ExecStart script paths resolve inside package ==="
# ExecStart comes in several shapes:
#   ExecStart=/usr/bin/python3 /opt/agora/src/player/main.py
#   ExecStart=/usr/bin/python3 -m cms_client.main
#   ExecStart=/bin/bash -c 'exec /usr/bin/python3 -u /opt/agora/src/provision/service.py ...'
# For script-path forms we assert the file exists inside the .deb. For
# `-m module` forms we rely on the "Import smoke: systemd entry points"
# step below — if the module is broken, that step will catch it.
for unit in "$STAGE"/etc/systemd/system/agora-*.service; do
    checked=0
    while read -r script; do
        [[ -z "$script" ]] && continue
        rel="${script#/}"
        if [[ ! -f "$STAGE/$rel" ]]; then
            echo "FAIL: $(basename "$unit") references $script but $STAGE/$rel missing"
            exit 1
        fi
        echo "  $(basename "$unit") → $script ✓"
        checked=1
    done < <(grep -oE '/opt/agora/src/[^ '"'"'"]+\.py' "$unit" || true)
    if [[ $checked -eq 0 ]]; then
        # No script path — must be `-m module`. Verified by import smoke below.
        echo "  $(basename "$unit") → (uses python -m, verified by import smoke)"
    fi
done

echo "=== Byte-compiling all shipped Python sources ==="
python3 -m compileall -q "$SRC"

echo "=== Creating venv + installing runtime requirements ==="
# --system-site-packages so the venv inherits apt-installed native
# bindings (python3-gi / python3-cairo / PangoCairo) which are what
# the Pi runtime uses and cannot be installed from pip wheels.
python3 -m venv --system-site-packages "$VENV"
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
# These four imports match the four systemd units' ExecStart targets
# (player/main.py, uvicorn api.main:app, cms_client.main, provision/service.py).
# If any top-level import chain is broken (like the v1.11.2 missing
# hardware/ module), one of these will fail the same way the Pi did.
for entry in player.main api.main cms_client.main provision.service; do
    python3 -c "import importlib; importlib.import_module('$entry'); print('ok: $entry')"
done

echo ""
echo "=== SMOKE PASSED ==="
