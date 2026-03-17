## OptionData Boot Chain

This bundle turns the recovered `OptionData` startup sequence into a persistent
systemd chain:

1. log in to the iSCSI target
2. mount the recovered XFS volume read-only at `/mnt/xfs_ro`
3. sync `meta.db` into `/opt/my_influx/meta`
4. start `bindfs` for data and WAL
5. mount the overlay views at:
   - `/opt/my_influx/data/OptionData`
   - `/opt/my_influx/wal/OptionData`
6. let `influxdb.service` depend on those mounts

Recovered manual command chain from `fish_history`:

```bash
sudo iscsiadm -m node -T iqn.2008-06.com.sysdevlabs:c0a888010cbc:vdev0 -p 192.168.136.1 --login
sudo mount -t xfs -o ro,norecovery /dev/sdc /mnt/xfs_ro
sudo bindfs --force-user=influxdb --force-group=influxdb -o ro /mnt/xfs_ro/influxdb/data/OptionData /mnt/mapped/data_target
sudo bindfs --force-user=influxdb --force-group=influxdb -o ro /mnt/xfs_ro/influxdb/wal/OptionData /mnt/mapped/wal_target
sudo mount -t overlay overlay -o lowerdir=/mnt/mapped/wal_target,upperdir=/opt/upper/wal,workdir=/opt/work/wal /opt/my_influx/wal/OptionData
sudo mount -t overlay overlay -o lowerdir=/mnt/mapped/data_target,upperdir=/opt/upper/data,workdir=/opt/work/data /opt/my_influx/data/OptionData
sudo systemctl start influxdb
```

This bundle uses the stable iSCSI by-path device instead of raw `/dev/sdc`:

```text
/dev/disk/by-path/ip-192.168.136.1:3260-iscsi-iqn.2008-06.com.sysdevlabs:c0a888010cbc:vdev0-lun-1
```

## Files

- `scripts/optiondata-prep-paths.sh`
- `scripts/optiondata-iscsi-login.sh`
- `scripts/optiondata-iscsi-logout.sh`
- `systemd/optiondata-prep.service`
- `systemd/optiondata-iscsi-login.service`
- `systemd/mnt-xfs_ro.mount`
- `systemd/optiondata-meta-sync.service`
- `systemd/bindfs-optiondata-data.service`
- `systemd/bindfs-optiondata-wal.service`
- `systemd/opt-my_influx-data-OptionData.mount`
- `systemd/opt-my_influx-wal-OptionData.mount`
- `systemd/influxdb-optiondata.conf`
- `install-optiondata-systemd.sh`

## Install

Review the files first, then run:

```bash
cd /home/niko/influx2parquet/deploy/optiondata-systemd
sudo bash install-optiondata-systemd.sh
```

Then enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable optiondata-prep.service
sudo systemctl enable optiondata-iscsi-login.service
sudo systemctl enable mnt-xfs_ro.mount
sudo systemctl enable optiondata-meta-sync.service
sudo systemctl enable bindfs-optiondata-data.service
sudo systemctl enable bindfs-optiondata-wal.service
sudo systemctl enable opt-my_influx-data-OptionData.mount
sudo systemctl enable opt-my_influx-wal-OptionData.mount
sudo systemctl restart optiondata-prep.service
sudo systemctl start optiondata-iscsi-login.service
sudo systemctl start mnt-xfs_ro.mount
sudo systemctl start optiondata-meta-sync.service
sudo systemctl start bindfs-optiondata-data.service
sudo systemctl start bindfs-optiondata-wal.service
sudo systemctl start opt-my_influx-data-OptionData.mount
sudo systemctl start opt-my_influx-wal-OptionData.mount
sudo systemctl restart influxdb
```

## Verify

```bash
systemctl --no-pager --full status optiondata-iscsi-login.service
systemctl --no-pager --full status mnt-xfs_ro.mount
systemctl --no-pager --full status bindfs-optiondata-data.service
systemctl --no-pager --full status bindfs-optiondata-wal.service
systemctl --no-pager --full status opt-my_influx-data-OptionData.mount
systemctl --no-pager --full status opt-my_influx-wal-OptionData.mount
systemctl --no-pager --full status influxdb

cat /proc/$(pgrep -f '/mnt/mapped/data_target')/limits | grep 'Max open files'
cat /proc/$(pgrep -f '/mnt/mapped/wal_target')/limits | grep 'Max open files'

mount | egrep '/mnt/xfs_ro|/mnt/mapped|/opt/my_influx'
```

Expected bindfs limit:

```text
Max open files            1048576              1048576              files
```
