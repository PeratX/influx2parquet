#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <mountpoint> [timeout-seconds]" >&2
  exit 2
fi

mountpoint_path="$1"
timeout_seconds="${2:-15}"
deadline=$((SECONDS + timeout_seconds))

is_mounted() {
  grep -qs " $mountpoint_path " /proc/self/mountinfo
}

while (( SECONDS < deadline )); do
  if is_mounted; then
    exit 0
  fi
  sleep 0.2
done

echo "Timed out waiting for mountpoint: $mountpoint_path" >&2
exit 1
