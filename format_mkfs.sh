#!/bin/bash
# Device map: <alias> -> <pcie_addr> <dev_name>
# Usage: ./format_mkfs.sh [samsung|wd] [append|zrwa]   (defaults: samsung append)
declare -A DEV_PCIE=( [samsung]="3b:00.0" [wd]="d8:00.0" )
declare -A DEV_NAME=( [samsung]="nvme1n1" [wd]="nvme0n2" )

DEV=${1:-samsung}
WAL_MODE=${2:-append}
if [ -z "${DEV_PCIE[$DEV]}" ]; then
  echo "Unknown device: $DEV  (available: ${!DEV_PCIE[*]})"
  exit 1
fi
case "$WAL_MODE" in
  append|zrwa) ;;
  *) echo "Unknown WAL mode: $WAL_MODE  (available: append, zrwa)"; exit 1 ;;
esac
PCIE_ADDR=${DEV_PCIE[$DEV]}
ZENFS_DEV=${DEV_NAME[$DEV]}

echo "Using device: $DEV  ($PCIE_ADDR / $ZENFS_DEV), wal_mode=$WAL_MODE"

if [ -d ./plugin/spdk ]
then
  SPDK_DIR=./plugin/spdk/scripts
else
  echo "please install SPDK v22.01.2"
  exit 1
fi

$SPDK_DIR/setup.sh > /dev/null 2> /dev/null

if [ ! -e ./plugin/zenfs/util/zenfs ]
then
cd plugin/zenfs/util
make
cd ../../..
fi

./plugin/zenfs/util/zenfs format --zbd=$ZENFS_DEV --aux_path=/tmp/aux_zenfs --zns_pci=$PCIE_ADDR --waltz_wal_mode=$WAL_MODE

rm -rf /tmp/aux_zenfs
./plugin/zenfs/util/zenfs mkfs --zbd=$ZENFS_DEV --aux_path=/tmp/aux_zenfs --zns_pci=$PCIE_ADDR --waltz_wal_mode=$WAL_MODE
