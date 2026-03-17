#!/usr/bin/env bash
set -euo pipefail

PORTAL="192.168.136.1"
TARGET="iqn.2008-06.com.sysdevlabs:c0a888010cbc:vdev0"

if /usr/sbin/iscsiadm -m session 2>/dev/null | grep -q "$TARGET"; then
  exit 0
fi

/usr/sbin/iscsiadm -m discoverydb -t sendtargets -p "$PORTAL" --discover >/dev/null 2>&1 || \
  /usr/sbin/iscsiadm -m discovery -t sendtargets -p "$PORTAL"

/usr/sbin/iscsiadm -m node -T "$TARGET" -p "$PORTAL" --op update -n node.startup -v automatic || true
/usr/sbin/iscsiadm -m node -T "$TARGET" -p "$PORTAL" --login
