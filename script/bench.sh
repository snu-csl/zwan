#!/bin/bash

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROCKSDB_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
DBBENCH_FILE="$ROCKSDB_ROOT/db_bench"

# Device map: <alias> -> <pcie_addr> <dev_name>
declare -A DEV_PCIE=( [samsung]="3b:00.0" [wd]="d8:00.0" )
declare -A DEV_NAME=( [samsung]="nvme1n1" [wd]="nvme0n2" )

TESTMODE=$1
TESTTYPE=$2
VALUESIZE=$3
KEYRANGE=$4
KEYTEST=$5
WALMODE=${6:-append}
THREADS=${7:-4}
DEV=${8:-samsung}

if [ -z "${DEV_PCIE[$DEV]}" ]; then
  echo "Unknown device: $DEV  (available: ${!DEV_PCIE[*]})"
  exit 1
fi
PCIE_ADDR=${DEV_PCIE[$DEV]}
ZENFS_DEV=${DEV_NAME[$DEV]}
MIXGRAPH_SKIP_VALUESIZE_ARGS=${MIXGRAPH_SKIP_VALUESIZE_ARGS:-0}
MIXGRAPH_SKIP_DIST_ARGS=${MIXGRAPH_SKIP_DIST_ARGS:-0}
RUN_DURATION_SECONDS=${RUN_DURATION_SECONDS:-0}

if [ "$TESTTYPE" -gt 5 ]; then
  TEST=mixgraph
else
  TEST=microbench
fi

BENCHMARKS="resetwafstats,prefill,printwafstats_prefill,resetwafstats,levelstats,resetstats,$TEST,printwafstats_workload,levelstats,stats"

if [ "$TESTMODE" -eq 0 ]; then
  BENCHMARKS="enablewaltzmode,$BENCHMARKS"
  OUT_PREFIX=${BENCH_OUT_PREFIX:-waltz}
else
  OUT_PREFIX=${BENCH_OUT_PREFIX:-zenfs}
fi
OUTFILE=stdout_${OUT_PREFIX}_test${TESTTYPE}.log
ERRFILE=stderr_${OUT_PREFIX}_test${TESTTYPE}.log

OPTIONS=""
OPTIONS+=" --waltz_test_type=$TESTTYPE"
OPTIONS+=" --waltz_zipf_dist=0.99"
OPTIONS+=" --waltz_scan_max=100"
OPTIONS+=" --waltz_key_range=$KEYRANGE"
OPTIONS+=" --waltz_pcie_addr=$PCIE_ADDR"
if [ "$TESTMODE" -eq 0 ]; then
  OPTIONS+=" --waltz_wal_mode=$WALMODE"
fi

OPTIONS+=" --fs_uri=zenfs://dev:$ZENFS_DEV"
OPTIONS+=" --max_background_compactions=4"
OPTIONS+=" --max_background_flushes=4"
OPTIONS+=" --max_background_jobs=8"
OPTIONS+=" --key_size=8"
OPTIONS+=" --prefix_size=8"
if [ "$TESTTYPE" -lt 6 ]; then
  OPTIONS+=" --value_size=$VALUESIZE"
fi
OPTIONS+=" --compression_type=none"
OPTIONS+=" --sync=true"
OPTIONS+=" --enable_pipelined_write=false"
OPTIONS+=" --statistics=1"
OPTIONS+=" --histogram=true"
OPTIONS+=" --use_direct_reads"
OPTIONS+=" --use_direct_io_for_flush_and_compaction"
OPTIONS+=" --memtablerep=skip_list"
OPTIONS+=" --bloom_bits=10"
OPTIONS+=" --bloom_locality=1"
OPTIONS+=" --stats_per_interval=0"
OPTIONS+=" --stats_interval_seconds=20"
OPTIONS+=" --benchmarks=$BENCHMARKS"
if [ "$RUN_DURATION_SECONDS" -gt 0 ]; then
  OPTIONS+=" --duration=$RUN_DURATION_SECONDS"
fi
if [ "$TESTTYPE" -lt 6 ]; then
  OPTIONS+=" --num=$((KEYTEST / THREADS))"
fi
OPTIONS+=" --threads=$THREADS"
OPTIONS+=" --open_files=500000"
OPTIONS+=" --use_existing_db=0"

if [ "$TESTTYPE" -gt 5 ]; then
  if [ "$TESTTYPE" -eq 7 ] || [ "$TESTTYPE" -eq 9 ]; then
    OPTIONS+=" --key_dist_a=0.002312"
    OPTIONS+=" --key_dist_b=0.3467"
  fi

  if [ "$TESTTYPE" -eq 6 ] || [ "$TESTTYPE" -eq 7 ]; then
    OPTIONS+=" --keyrange_num=1"
  else
    OPTIONS+=" --keyrange_dist_a=14.18"
    OPTIONS+=" --keyrange_dist_b=-2.917"
    OPTIONS+=" --keyrange_dist_c=0.0164"
    OPTIONS+=" --keyrange_dist_d=-0.08082"
    OPTIONS+=" --keyrange_num=30"
  fi

  if [ "$MIXGRAPH_SKIP_VALUESIZE_ARGS" -ne 1 ]; then
    OPTIONS+=" --value_theta=$VALUESIZE"
    OPTIONS+=" --mix_max_value_size=$VALUESIZE"
  fi
  if [ "$MIXGRAPH_SKIP_DIST_ARGS" -ne 1 ]; then
    OPTIONS+=" --value_sigma=0"
    OPTIONS+=" --iter_k=2.517"
    OPTIONS+=" --iter_sigma=14.236"
  fi
  OPTIONS+=" --mix_get_ratio=0.83"
  OPTIONS+=" --mix_put_ratio=0.14"
  OPTIONS+=" --mix_seek_ratio=0.03"
  OPTIONS+=" --sine_mix_rate_interval_milliseconds=5000"
  OPTIONS+=" --num=$KEYRANGE"
  OPTIONS+=" --reads=$((KEYTEST / THREADS))"
  OPTIONS+=" --sine_a=0"
  OPTIONS+=" --sine_b=0"
  OPTIONS+=" --sine_d=0"
fi

if [ -n "${EXTRA_BENCH_OPTS:-}" ]; then
  OPTIONS+=" $EXTRA_BENCH_OPTS"
fi

cd "$SCRIPT_DIR"
"$DBBENCH_FILE" $OPTIONS >"$OUTFILE" 2>"$ERRFILE"
echo "$OPTIONS" >>"$ERRFILE"
