#!/usr/bin/env bash
set -euo pipefail

  is_mounted() {
    local path="$1"
    grep -qs " $path " /proc/self/mountinfo
  }

  ensure_dir() {
    local path="$1"
    if is_mounted "$path"; then
      return 0
    fi
    mkdir -p "$path"
  }

  set_owner_if_plain_dir() {
    local path="$1"
    if is_mounted "$path"; then
      return 0
    fi
    chown influxdb:influxdb "$path"
    chmod 0755 "$path"
  }

  clear_dir_contents() {
    local path="$1"
    if is_mounted "$path"; then
      echo "refusing to clear mounted path: $path" >&2
      exit 1
    fi
    if [[ -d "$path" ]]; then
      find "$path" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    fi
  }

  ensure_dir /mnt/xfs_ro
  ensure_dir /mnt/mapped
  ensure_dir /mnt/mapped/data_target
  ensure_dir /mnt/mapped/wal_target

  ensure_dir /opt/my_influx
  ensure_dir /opt/my_influx/meta
  ensure_dir /opt/my_influx/data
  ensure_dir /opt/my_influx/wal
  ensure_dir /opt/my_influx/data/OptionData
  ensure_dir /opt/my_influx/wal/OptionData

  ensure_dir /opt/upper
  ensure_dir /opt/upper/data
  ensure_dir /opt/upper/wal
  ensure_dir /opt/work
  ensure_dir /opt/work/data
  ensure_dir /opt/work/wal

  clear_dir_contents /opt/upper/data
  clear_dir_contents /opt/upper/wal
  clear_dir_contents /opt/work/data
  clear_dir_contents /opt/work/wal
  rm -f /opt/my_influx/meta/meta.db

  set_owner_if_plain_dir /opt/my_influx
  set_owner_if_plain_dir /opt/my_influx/meta
  set_owner_if_plain_dir /opt/my_influx/data
  set_owner_if_plain_dir /opt/my_influx/wal
  set_owner_if_plain_dir /opt/my_influx/data/OptionData
  set_owner_if_plain_dir /opt/my_influx/wal/OptionData

  set_owner_if_plain_dir /opt/upper
  set_owner_if_plain_dir /opt/upper/data
  set_owner_if_plain_dir /opt/upper/wal
  set_owner_if_plain_dir /opt/work
  set_owner_if_plain_dir /opt/work/data
  set_owner_if_plain_dir /opt/work/wal
