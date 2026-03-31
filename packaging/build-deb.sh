#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Build the Agora .deb package
# Usage: bash packaging/build-deb.sh [version]
# If version is omitted, reads from api/__init__.py
# ──────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Version ──
VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
    VERSION=$(python3 -c "
import re, pathlib
text = pathlib.Path('${REPO_ROOT}/api/__init__.py').read_text()
print(re.search(r'__version__\s*=\s*\"(.+?)\"', text).group(1))
")
fi
echo "Building agora ${VERSION}"

# ── Workspace ──
PKG="agora"
ARCH="arm64"
BUILD_DIR="${REPO_ROOT}/build/${PKG}_${VERSION}_${ARCH}"

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}/DEBIAN"
mkdir -p "${BUILD_DIR}/opt/agora/src"
mkdir -p "${BUILD_DIR}/opt/agora/assets/splash"
mkdir -p "${BUILD_DIR}/opt/agora/state"
mkdir -p "${BUILD_DIR}/opt/agora/persist"
mkdir -p "${BUILD_DIR}/opt/agora/logs"
mkdir -p "${BUILD_DIR}/opt/agora/tmp"
mkdir -p "${BUILD_DIR}/etc/systemd/system"
mkdir -p "${BUILD_DIR}/usr/share/plymouth/themes/splash"

# ── Source code ──
for dir in api player shared cms_client provision; do
    cp -r "${REPO_ROOT}/${dir}" "${BUILD_DIR}/opt/agora/src/"
done
cp "${REPO_ROOT}/requirements-api.txt" "${BUILD_DIR}/opt/agora/src/"
cp "${REPO_ROOT}/requirements-player.txt" "${BUILD_DIR}/opt/agora/src/"
cp "${REPO_ROOT}/requirements-cms-client.txt" "${BUILD_DIR}/opt/agora/src/"

# ── Default splash ──
if [[ -f "${REPO_ROOT}/config/boot-splash.png" ]]; then
    cp "${REPO_ROOT}/config/boot-splash.png" "${BUILD_DIR}/opt/agora/assets/splash/default.png"
fi

# ── Systemd units ──
cp "${REPO_ROOT}/systemd/agora-api.service" "${BUILD_DIR}/etc/systemd/system/"
cp "${REPO_ROOT}/systemd/agora-player.service" "${BUILD_DIR}/etc/systemd/system/"
cp "${REPO_ROOT}/systemd/agora-cms-client.service" "${BUILD_DIR}/etc/systemd/system/"
cp "${REPO_ROOT}/systemd/agora-provision.service" "${BUILD_DIR}/etc/systemd/system/"

# ── Plymouth theme ──
if [[ -f "${REPO_ROOT}/config/boot-splash.png" ]]; then
    cp "${REPO_ROOT}/config/boot-splash.png" \
       "${BUILD_DIR}/usr/share/plymouth/themes/splash/splash.png"
fi
cp "${REPO_ROOT}/packaging/plymouth/splash.plymouth" \
   "${BUILD_DIR}/usr/share/plymouth/themes/splash/"
cp "${REPO_ROOT}/packaging/plymouth/splash.script" \
   "${BUILD_DIR}/usr/share/plymouth/themes/splash/"

# ── DEBIAN control files ──
sed "s/@@VERSION@@/${VERSION}/g" "${SCRIPT_DIR}/debian/control.in" \
    > "${BUILD_DIR}/DEBIAN/control"
cp "${SCRIPT_DIR}/debian/conffiles" "${BUILD_DIR}/DEBIAN/"
cp "${SCRIPT_DIR}/debian/postinst"  "${BUILD_DIR}/DEBIAN/"
cp "${SCRIPT_DIR}/debian/prerm"     "${BUILD_DIR}/DEBIAN/"
cp "${SCRIPT_DIR}/debian/postrm"    "${BUILD_DIR}/DEBIAN/"
chmod 755 "${BUILD_DIR}/DEBIAN/postinst"
chmod 755 "${BUILD_DIR}/DEBIAN/prerm"
chmod 755 "${BUILD_DIR}/DEBIAN/postrm"

# ── Build ──
DEB_FILE="${REPO_ROOT}/build/${PKG}_${VERSION}_${ARCH}.deb"
dpkg-deb --build --root-owner-group "${BUILD_DIR}" "${DEB_FILE}"
echo "Built: ${DEB_FILE}"
