#!/bin/bash
set -euo pipefail
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
mkdir -p /run/munge /run/mysqld /var/spool/slurmctld /var/spool/slurmd /var/log/slurm /etc/slurm
chown munge:munge /run/munge
chown mysql:mysql /run/mysqld
/usr/sbin/munged --force
/usr/sbin/mariadbd --user=mysql --skip-networking > /var/log/mariadb-test.log 2>&1 &
for i in $(seq 1 50); do mysqladmin ping >/dev/null 2>&1 && break; sleep .2; done
mysql -e "CREATE DATABASE IF NOT EXISTS slurm_acct_db; CREATE USER IF NOT EXISTS 'slurm'@'localhost' IDENTIFIED BY 'disposable-test'; GRANT ALL ON slurm_acct_db.* TO 'slurm'@'localhost';"
cat > /etc/slurm/slurmdbd.conf <<'EOF'
AuthType=auth/munge
DbdHost=localhost
SlurmUser=root
StorageType=accounting_storage/mysql
StorageHost=localhost
StorageUser=slurm
StoragePass=disposable-test
StorageLoc=slurm_acct_db
LogFile=/var/log/slurm/slurmdbd.log
PidFile=/run/slurmdbd.pid
EOF
chmod 600 /etc/slurm/slurmdbd.conf
hardware=$(/usr/sbin/slurmd -C | head -1)
cat > /etc/slurm/slurm.conf <<EOF
ClusterName=adagio-disposable
SlurmctldHost=slurm
SlurmUser=root
AuthType=auth/munge
StateSaveLocation=/var/spool/slurmctld
SlurmdSpoolDir=/var/spool/slurmd
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.log
SchedulerType=sched/backfill
SelectType=select/cons_tres
SelectTypeParameters=CR_Core_Memory
TaskPlugin=task/none
ProctrackType=proctrack/linuxproc
JobAcctGatherType=jobacct_gather/linux
AccountingStorageType=accounting_storage/slurmdbd
AccountingStorageHost=localhost
ReturnToService=2
$hardware
PartitionName=test Nodes=slurm Default=YES MaxTime=00:10:00 State=UP
EOF
# Neither test setup enforces memory cgroups; do not claim OOM enforcement.
if slurmd -V | grep -q 'slurm 23'; then
    echo CgroupPlugin=cgroup/v1 > /etc/slurm/cgroup.conf
else
    echo CgroupPlugin=disabled > /etc/slurm/cgroup.conf
fi
/usr/sbin/slurmdbd
sleep 2
/usr/sbin/slurmctld
/usr/sbin/slurmd
sacctmgr -i add cluster adagio-disposable
sacctmgr -i add account test Cluster=adagio-disposable
sacctmgr -i add user root Account=test Cluster=adagio-disposable
sinfo
