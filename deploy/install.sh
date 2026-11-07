#!/usr/bin/env bash
# Install darwin-vk as a root LaunchDaemon. Run with sudo from the repo root after `make build`.
#   sudo deploy/install.sh <darwin-vk.kubeconfig> <cluster-ca.crt> [extra darwin-vk flags...]
#   sudo deploy/install.sh --uninstall
set -euo pipefail
LABEL=io.github.changtimwu.darwin-vk
PLIST=/Library/LaunchDaemons/$LABEL.plist
BIN_DIR=/usr/local/libexec/darwin-vk
ETC=/etc/darwin-vk
POD_USER=_darwinvk

[ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }

if [ "${1:-}" = "--uninstall" ]; then
  launchctl bootout system/$LABEL 2>/dev/null || true
  rm -f "$PLIST"
  # Stop pod process groups the daemon left running (they outlive it by design).
  pkill -TERM -f "$BIN_DIR/k3sm-execshim" 2>/dev/null || true
  sleep 10; pkill -KILL -f "$BIN_DIR/k3sm-execshim" 2>/dev/null || true
  rm -rf "$BIN_DIR"
  echo "uninstalled (kept $ETC, /var/lib/darwin-vk and user $POD_USER)"
  exit 0
fi

KUBECONFIG_SRC=${1:?kubeconfig path}; CA_SRC=${2:?cluster CA path}; shift 2

# Dedicated, login-less service user that pods without a securityContext run as.
if ! id "$POD_USER" >/dev/null 2>&1; then
  uid=$(dscl . -list /Users UniqueID | awk '{print $2}' | sort -n | awk '$1>=400 && $1<500' | tail -1)
  uid=$(( ${uid:-399} + 1 ))
  dscl . -create /Groups/$POD_USER PrimaryGroupID $uid
  dscl . -create /Users/$POD_USER UniqueID $uid
  dscl . -create /Users/$POD_USER PrimaryGroupID $uid
  dscl . -create /Users/$POD_USER UserShell /usr/bin/false
  dscl . -create /Users/$POD_USER NFSHomeDirectory /var/empty
  dscl . -create /Users/$POD_USER IsHidden 1
  echo "created user $POD_USER (uid $uid)"
fi

install -d -m 755 "$BIN_DIR"
install -m 755 bin/darwin-vk bin/k3sm-execshim bin/libk3sm_pathrebase_shim.dylib "$BIN_DIR/"
# Re-signed shells and coreutils, so /bin/sh pods see their volume mounts.
"$BIN_DIR/darwin-vk" install-shadow-bin "$BIN_DIR/shadow-bin"
install -d -m 700 "$ETC"
install -m 600 "$KUBECONFIG_SRC" "$ETC/darwin-vk.kubeconfig"
install -m 644 "$CA_SRC" "$ETC/cluster-ca.crt"
install -d -m 755 /var/lib/darwin-vk

cp deploy/launchd/$LABEL.plist "$PLIST"
for flag in "$@"; do /usr/libexec/PlistBuddy -c "Add :ProgramArguments: string $flag" "$PLIST"; done
chown root:wheel "$PLIST"; chmod 644 "$PLIST"
launchctl bootout system/$LABEL 2>/dev/null || true
# bootout returns before the old daemon has exited; bootstrapping too early fails with EIO (5).
for _ in $(seq 30); do launchctl print system/$LABEL >/dev/null 2>&1 || break; sleep 1; done
launchctl bootstrap system "$PLIST"
echo "started $LABEL; logs: /var/log/darwin-vk.log"
