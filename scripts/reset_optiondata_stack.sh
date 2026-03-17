#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run this script as root, for example:" >&2
  echo "  sudo $0" >&2
  exit 1
fi

is_mounted() {
  local path="$1"
  grep -qs " $path " /proc/self/mountinfo
}

force_unmount() {
  local path="$1"
  if ! is_mounted "$path"; then
    return 0
  fi
  umount "$path" 2>/dev/null || true
  if is_mounted "$path"; then
    fusermount3 -u "$path" 2>/dev/null || true
  fi
  if is_mounted "$path"; then
    echo "Failed to unmount $path" >&2
    exit 1
  fi
}

clear_dir_contents() {
  local path="$1"
  if is_mounted "$path"; then
    echo "Refusing to clear mounted path: $path" >&2
    exit 1
  fi
  if [[ -d "$path" ]]; then
    find "$path" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
}

echo "[1/5] Stopping active exporters so mount teardown does not race..."
pkill -INT -f 'export_okex_depth_tsm.py' 2>/dev/null || true
pkill -INT -f 'extract_optiondata_to_parquet.py' 2>/dev/null || true
pkill -INT -f 'export_parallel_instid.py' 2>/dev/null || true
sleep 2

echo "[2/5] Stopping InfluxDB and overlay/bindfs services..."
systemctl stop influxdb 2>/dev/null || true
systemctl stop opt-my_influx-data-OptionData.mount 2>/dev/null || true
systemctl stop opt-my_influx-wal-OptionData.mount 2>/dev/null || true
systemctl stop bindfs-optiondata-data.service 2>/dev/null || true
systemctl stop bindfs-optiondata-wal.service 2>/dev/null || true

echo "[3/5] Forcing mount teardown and clearing overlay upper/work state..."
force_unmount /opt/my_influx/data/OptionData
force_unmount /opt/my_influx/wal/OptionData
force_unmount /mnt/mapped/data_target
force_unmount /mnt/mapped/wal_target

clear_dir_contents /opt/upper/data
clear_dir_contents /opt/upper/wal
clear_dir_contents /opt/work/data
clear_dir_contents /opt/work/wal
rm -f /opt/my_influx/meta/meta.db

echo "[4/5] Restoring read-only recovery stack..."
systemctl start optiondata-iscsi-login.service
systemctl start mnt-xfs_ro.mount
systemctl restart optiondata-meta-sync.service
systemctl start bindfs-optiondata-data.service
systemctl start bindfs-optiondata-wal.service
systemctl start opt-my_influx-data-OptionData.mount
systemctl start opt-my_influx-wal-OptionData.mount

echo "[5/5] Final status (InfluxDB intentionally left stopped)..."
systemctl --no-pager --full status bindfs-optiondata-data.service || true
systemctl --no-pager --full status bindfs-optiondata-wal.service || true
systemctl --no-pager --full status opt-my_influx-data-OptionData.mount || true
systemctl --no-pager --full status opt-my_influx-wal-OptionData.mount || true
systemctl is-active influxdb || true

cat <<'EOF'

Reset complete.

- InfluxDB is left stopped on purpose.
- Scan/export progress already written to checkpoint/state files is preserved.
- Any in-flight file that had not completed will be retried on the next run.

To resume the TSM scan/export pipeline, rerun your previous command, for example:

  sudo -u influxdb /home/niko/influx2parquet/.venv/bin/python \
    /home/niko/influx2parquet/python/export_okex_depth_tsm.py \
    --output-dir /mnt/backup_hdd/exported_parallel \
    --workers 6 \
    --stop-after scan

EOF
