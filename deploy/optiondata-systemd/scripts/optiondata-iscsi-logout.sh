#!/usr/bin/env bash
set -euo pipefail

PORTAL="192.168.136.1"
TARGET="iqn.2008-06.com.sysdevlabs:c0a888010cbc:vdev0"

if /usr/sbin/iscsiadm -m session 2>/dev/null | grep -q "$TARGET"; then
  /usr/sbin/iscsiadm -m node -T "$TARGET" -p "$PORTAL" --logout || true
fi
