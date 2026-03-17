#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

install -d /usr/local/libexec
install -d /etc/systemd/system
install -d /etc/systemd/system/influxdb.service.d

install -m 0755 "$ROOT_DIR/scripts/optiondata-prep-paths.sh" /usr/local/libexec/optiondata-prep-paths.sh
install -m 0755 "$ROOT_DIR/scripts/optiondata-iscsi-login.sh" /usr/local/libexec/optiondata-iscsi-login.sh
install -m 0755 "$ROOT_DIR/scripts/optiondata-iscsi-logout.sh" /usr/local/libexec/optiondata-iscsi-logout.sh
install -m 0755 "$ROOT_DIR/scripts/optiondata-wait-for-mount.sh" /usr/local/libexec/optiondata-wait-for-mount.sh

install -m 0644 "$ROOT_DIR/systemd/optiondata-prep.service" /etc/systemd/system/optiondata-prep.service
install -m 0644 "$ROOT_DIR/systemd/optiondata-iscsi-login.service" /etc/systemd/system/optiondata-iscsi-login.service
install -m 0644 "$ROOT_DIR/systemd/mnt-xfs_ro.mount" /etc/systemd/system/mnt-xfs_ro.mount
install -m 0644 "$ROOT_DIR/systemd/optiondata-meta-sync.service" /etc/systemd/system/optiondata-meta-sync.service
install -m 0644 "$ROOT_DIR/systemd/bindfs-optiondata-data.service" /etc/systemd/system/bindfs-optiondata-data.service
install -m 0644 "$ROOT_DIR/systemd/bindfs-optiondata-wal.service" /etc/systemd/system/bindfs-optiondata-wal.service
install -m 0644 "$ROOT_DIR/systemd/opt-my_influx-data-OptionData.mount" /etc/systemd/system/opt-my_influx-data-OptionData.mount
install -m 0644 "$ROOT_DIR/systemd/opt-my_influx-wal-OptionData.mount" /etc/systemd/system/opt-my_influx-wal-OptionData.mount
install -m 0644 "$ROOT_DIR/systemd/influxdb-optiondata.conf" /etc/systemd/system/influxdb.service.d/optiondata.conf

echo "Installed OptionData systemd files."
echo "Next:"
echo "  sudo systemctl daemon-reload"
echo "  sudo systemctl enable optiondata-prep.service optiondata-iscsi-login.service mnt-xfs_ro.mount optiondata-meta-sync.service bindfs-optiondata-data.service bindfs-optiondata-wal.service opt-my_influx-data-OptionData.mount opt-my_influx-wal-OptionData.mount"
