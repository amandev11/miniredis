#!/usr/bin/env bash
# Install the Go toolchain that this project is developed and verified with.
#
# The project targets Go 1.24 or newer (see go.mod). This script installs a
# pinned toolchain into $HOME/tools/go<version> so that no root privileges are
# required. Verified with go1.27.3 on linux/amd64.
#
# Usage:  scripts/install-go.sh [version]
set -euo pipefail

VERSION="${1:-1.27.3}"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64) ARCH="amd64" ;;
  aarch64 | arm64) ARCH="arm64" ;;
  *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

DEST="${HOME}/tools/go${VERSION}"
TARBALL="go${VERSION}.${OS}-${ARCH}.tar.gz"
URL="https://go.dev/dl/${TARBALL}"

if [ -x "${DEST}/bin/go" ]; then
  echo "already installed: $("${DEST}/bin/go" version)"
  exit 0
fi

mkdir -p "${HOME}/tools"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

echo "downloading ${URL}"
curl -fsSL -o "${TMP}/${TARBALL}" "${URL}"
tar -C "${TMP}" -xzf "${TMP}/${TARBALL}"
rm -rf "${DEST}"
mv "${TMP}/go" "${DEST}"

echo "installed: $("${DEST}/bin/go" version)"
echo
echo "Add it to your PATH for this shell:"
echo "  export PATH=\"${DEST}/bin:\$PATH\""
