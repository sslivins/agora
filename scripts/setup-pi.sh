#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Agora — Raspberry Pi Setup Script
# Supports: Pi Zero 2 W, Pi 4, Pi 5/CM5
# Base OS: Raspberry Pi OS 64-bit Lite
#
# Run: curl -fsSL https://raw.githubusercontent.com/sslivins/agora/main/scripts/setup-pi.sh | sudo bash
# ──────────────────────────────────────────────────────────────
set -euo pipefail

REPO_URL="https://sslivins.github.io/agora"
REPO_LIST="/etc/apt/sources.list.d/agora.list"

# ── Colors ──
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Preflight ──
[[ $EUID -eq 0 ]] || error "This script must be run as root (sudo)"
[[ "$(uname -m)" == "aarch64" ]] || warn "Expected aarch64 — are you on a supported Raspberry Pi?"

info "Starting Agora setup on $(hostname)"

# ── 1. Add Agora apt repository ──
info "Adding Agora apt repository"
echo "deb [arch=arm64 trusted=yes] ${REPO_URL} stable main" > "${REPO_LIST}"
apt-get update -qq

# ── 2. Install Agora ──
info "Installing Agora"
apt-get install -y agora

# ── 3. Verify ──
sleep 3
echo ""
info "=== Service Status ==="
systemctl is-active agora-provision && info "agora-provision: running" || warn "agora-provision: not running"
systemctl is-active agora-player && info "agora-player: running" || warn "agora-player: not running"
systemctl is-active agora-api && info "agora-api: running" || warn "agora-api: not running"

echo ""
info "=== Setup Complete ==="
info "If this is a fresh device, connect to the Agora-XXXX Wi-Fi network"
info "to configure Wi-Fi and CMS settings via the captive portal."
info ""
info "Web UI:  http://$(hostname -I | awk '{print $1}'):8000"
info "Login:   admin / agora"
info "Config:  /boot/agora-config.json"
info ""
info "To upgrade later:  sudo apt update && sudo apt upgrade agora"
echo ""
