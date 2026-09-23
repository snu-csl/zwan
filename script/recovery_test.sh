#!/bin/bash
# Crash-recovery test: format, fillrandom, SIGKILL the workload at TARGET_OPS total
# ops, then reopen the DB and record the recovery time.
#
# Usage: sudo -E env TARGET_OPS=2000000 bash script/recovery_test.sh [device] [testtype] [modes...]
# Env: TARGET_OPS (2000000), THREADS (4), STATS_POLL_S (0.05), WORKLOAD_DURATION (1800),
#      PREFILL_KEYS (64000000), RESULT_DIR (script/results/<ts>_recovery_<device>_t<testtype>)

set -uo pipefail

DEVICE=${1:-samsung}
TESTTYPE=${2:-9}
shift $(( $# > 2 ? 2 : $# ))
MODES=${*:-"zenfs append zrwa"}

case "$DEVICE" in samsung|wd) ;; *) echo "Unknown device: $DEVICE" >&2; exit 1 ;; esac
for m in $MODES; do
  case "$m" in zenfs|append|zrwa) ;; *) echo "Unknown mode: $m" >&2; exit 1 ;; esac
done

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROCKSDB_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
DB_BENCH="$ROCKSDB_ROOT/db_bench"

declare -A DEV_PCIE=( [samsung]="3b:00.0" [wd]="d8:00.0" )
declare -A DEV_NAME=( [samsung]="nvme1n1" [wd]="nvme0n2" )
PCIE_ADDR=${DEV_PCIE[$DEVICE]}
ZNS_DEV=${DEV_NAME[$DEVICE]}

TARGET_OPS=${TARGET_OPS:-2000000}
THREADS=${THREADS:-4}
# Poll interval, real granularity is bounded by the awk re-parse of the stderr file
STATS_POLL_S=${STATS_POLL_S:-0.05}
# Op granularity of db_bench stats emission, smaller bounds the kill overshoot
STATS_INTERVAL_OPS=${STATS_INTERVAL_OPS:-10000}
WORKLOAD_DURATION=${WORKLOAD_DURATION:-1800}
PREFILL_KEYS=${PREFILL_KEYS:-64000000}

TS=$(date +%Y%m%d_%H%M%S)
RESULT_DIR=${RESULT_DIR:-"$SCRIPT_DIR/results/${TS}_recovery_${DEVICE}_t${TESTTYPE}"}
mkdir -p "$RESULT_DIR"
LOG="$RESULT_DIR/run.log"
SUMMARY="$RESULT_DIR/summary.tsv"
printf "mode\tprefill_exit\tprefill_secs\tworkload_exit\tworkload_secs\ttarget_ops\treached_ops\trecover_exit\trecover_wall_secs\trecover_dbopen_ms\twal_replay_ms\twal_files\trecover_status\trecovered_seq\tintegrity\treopen\twal_read_mb\twal_read_ios\n" >"$SUMMARY"

ulimit -n 100000

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }
drop_cache() { sync; echo 3 >/proc/sys/vm/drop_caches; }
time_cmd() {
  local out=$1; shift; local err=$1; shift
  local t0 t1; t0=$(date +%s.%N); "$@" >"$out" 2>"$err"; local rc=$?
  t1=$(date +%s.%N)
  awk -v s="$t0" -v e="$t1" 'BEGIN{printf "%.3f",e-s}'
  return $rc
}

mode_to_walmode() { case "$1" in zenfs) echo append;; append) echo append;; zrwa) echo zrwa;; esac; }
mode_to_testmode() { case "$1" in zenfs) echo 1;; append|zrwa) echo 0;; esac; }
mode_to_extra() {
  local opts=""
  if [[ "$1" == "zrwa" && "$DEVICE" == "wd" ]]; then opts+=" --zrwa_exp_flush=false"; fi
  echo "$opts"
}
mode_bench_prefix() {
  case "$1" in zenfs) echo "";; append|zrwa) echo "enablewaltzmode,";; esac
}

common_opts() {
  local mode=$1
  local walmode testmode
  walmode=$(mode_to_walmode "$mode"); testmode=$(mode_to_testmode "$mode")
  cat <<EOF
--waltz_pcie_addr=$PCIE_ADDR
--fs_uri=zenfs://dev:$ZNS_DEV
--key_size=8
--prefix_size=8
--compression_type=none
--sync=true
--enable_pipelined_write=false
--use_direct_reads
--use_direct_io_for_flush_and_compaction
--memtablerep=skip_list
--bloom_bits=10
--bloom_locality=1
--max_background_compactions=4
--max_background_flushes=4
--max_background_jobs=8
--open_files=500000
--statistics=0
--histogram=false
--stats_per_interval=0
--stats_interval=$STATS_INTERVAL_OPS
--waltz_key_range=$PREFILL_KEYS
EOF
  if [ "$testmode" = "0" ]; then
    echo "--waltz_wal_mode=$walmode"
  fi
  echo "$(mode_to_extra "$mode")"
}

# Sum the max cumulative ops per thread from db_bench stats lines:
#   "thread N: (interval_ops, cumulative_ops) ops and ..."
parse_total_ops() {
  local f=$1
  awk '
    match($0, /thread ([0-9]+):/, t) {
      tid=t[1]
      if (match($0, /\(([0-9]+),([0-9]+)\)/, m)) {
        cum=m[2]+0
        if (!(tid in best) || cum>best[tid]) best[tid]=cum
      }
    }
    END { sum=0; for (k in best) sum+=best[k]; print sum }
  ' "$f" 2>/dev/null
}

run_mode() {
  local mode=$1
  local outdir="$RESULT_DIR/$mode"; mkdir -p "$outdir"
  local walmode prefix cmn
  walmode=$(mode_to_walmode "$mode"); prefix=$(mode_bench_prefix "$mode")
  cmn=$(common_opts "$mode")

  log
  log "============================================================"
  log "MODE = $mode  (walmode=$walmode, target_ops=$TARGET_OPS)"
  log "============================================================"

  log "[1/3] format ZNS (waltz_wal_mode=$walmode)"
  rm -f /var/tmp/spdk_pci_lock*
  local fmt_t fmt_rc
  fmt_t=$(time_cmd "$outdir/format.log" "$outdir/format.err" \
    bash -c "cd '$ROCKSDB_ROOT' && ./format_mkfs.sh '$DEVICE' '$walmode'")
  fmt_rc=$?
  log "  format done (${fmt_t}s) exit=$fmt_rc"
  if [ "$fmt_rc" -ne 0 ]; then
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$mode" "NA" "NA" "NA" "NA" "$TARGET_OPS" "NA" "NA" "NA" "NA" "NA" "NA" "format_failed" "NA" "NA" "NA" "NA" "NA" >>"$SUMMARY"
    return 1
  fi
  drop_cache

  log "[2/3] fillrandom, kill when total ops >= $TARGET_OPS"
  local wl_start wl_end
  wl_start=$(date +%s.%N)
  : >"$outdir/workload.stdout"; : >"$outdir/workload.stderr"
  "$DB_BENCH" $cmn \
    --benchmarks="${prefix}resetwafstats,fillrandom,printwafstats_workload,levelstats,stats" \
    --num=$((PREFILL_KEYS * 100)) \
    --threads=$THREADS \
    --use_existing_db=0 \
    --duration=$WORKLOAD_DURATION \
    >"$outdir/workload.stdout" 2>"$outdir/workload.stderr" &
  local bench_pid=$!
  log "  workload pid=$bench_pid (poll every ${STATS_POLL_S}s)"

  local reached=0 last_log=0
  while kill -0 "$bench_pid" 2>/dev/null; do
    sleep "$STATS_POLL_S"
    reached=$(parse_total_ops "$outdir/workload.stderr")
    [ -z "$reached" ] && reached=0
    # log every ~10s
    local now=$(date +%s)
    if [ $((now - last_log)) -ge 10 ]; then
      log "  progress: total_ops=$reached / $TARGET_OPS"
      last_log=$now
    fi
    if [ "$reached" -ge "$TARGET_OPS" ]; then
      log "  TARGET_OPS reached ($reached >= $TARGET_OPS), sending SIGKILL"
      kill -KILL "$bench_pid" 2>/dev/null
      break
    fi
  done
  wait "$bench_pid" 2>/dev/null
  local wl_rc=$?
  wl_end=$(date +%s.%N)
  local wl_t
  wl_t=$(awk -v s="$wl_start" -v e="$wl_end" 'BEGIN{printf "%.3f",e-s}')
  reached=$(parse_total_ops "$outdir/workload.stderr")
  [ -z "$reached" ] && reached=0
  log "  workload reaped (${wl_t}s, exit=$wl_rc, reached_ops=$reached)"

  sleep 3
  rm -f /var/tmp/spdk_pci_lock*
  drop_cache

  log "[3/3] reopen DB, recovery"
  local rc_t rc_rc
  rc_t=$(time_cmd "$outdir/recover.stdout" "$outdir/recover.stderr" \
    "$DB_BENCH" $cmn \
      --benchmarks="${prefix}sstables,levelstats,stats" \
      --threads=1 \
      --use_existing_db=1 \
      --report_open_timing=true)
  rc_rc=$?
  log "  reopen done (${rc_t}s) exit=$rc_rc"

  local dbopen_ms="NA" wal_ms="NA" wal_n="NA"
  if [ -s "$outdir/recover.stdout" ]; then
    dbopen_ms=$(grep -m1 -E '^OpenDb:' "$outdir/recover.stdout" | awk '{print $2}')
    [ -z "$dbopen_ms" ] && dbopen_ms="NA"
  fi
  if [ -s "$outdir/recover.stderr" ]; then
    line=$(grep -m1 -E '^\[WALTZ\] RecoverLogFiles:' "$outdir/recover.stderr")
    if [ -n "$line" ]; then
      wal_ms=$(echo "$line" | awk '{print $3}')
      wal_n=$(echo "$line" | sed -n 's/.*wal_files=\([0-9]*\).*/\1/p')
      [ -z "$wal_n" ] && wal_n="NA"
    fi
  fi
  local wal_read_mb="NA" wal_read_ios="NA"
  if [ -s "$outdir/recover.stderr" ]; then
    rline=$(grep -m1 -E '^\[WAL_READ\]' "$outdir/recover.stderr")
    if [ -n "$rline" ]; then
      wal_read_mb=$(echo "$rline" | sed -n 's/.*dev_bytes=\([0-9]*\).*/\1/p' | awk '{printf "%.1f", $1/1048576}')
      wal_read_ios=$(echo "$rline" | sed -n 's/.*ios=\([0-9]*\).*/\1/p')
      [ -z "$wal_read_mb" ] && wal_read_mb="NA"
      [ -z "$wal_read_ios" ] && wal_read_ios="NA"
    fi
  fi
  log "  RecoverLogFiles: ${wal_ms} ms (wal_files=${wal_n})"
  log "  DB::Open total : ${dbopen_ms} ms"
  log "  WAL dev read   : ${wal_read_mb} MB / ${wal_read_ios} ios"

  local rc_status
  if [ "$rc_rc" -eq 0 ]; then rc_status="ok"; log "  RECOVERY OK"
  else rc_status="failed"; log "  RECOVERY FAILED"; fi

  if grep -qE "Corruption|Fatal|abort|terminate|Failed to mount|WAL mode mismatch" \
       "$outdir/recover.stderr" 2>/dev/null; then
    log "  WARN: error keywords in recover.stderr:"
    grep -nE "Corruption|Fatal|abort|terminate|Failed to mount|WAL mode mismatch" \
      "$outdir/recover.stderr" | head -5 | sed 's/^/    /' | tee -a "$LOG"
    if [ "$rc_status" = "ok" ]; then rc_status="ok_with_warnings"; fi
  fi

  # rec_seq comes from the recovery open's sstables dump, before any post-open damage
  # The readonly reopen only records whether the recovered DB opens again
  local rec_seq="NA" integ="NA" reopen="NA"
  if [ "$rc_rc" -eq 0 ]; then
    rec_seq=$(grep -oE '\[[0-9]+ \.\. [0-9]+\]' "$outdir/recover.stdout" \
      | awk -F'[^0-9]+' '{for(i=1;i<=NF;i++) if($i!="" && $i+0>m) m=$i+0} END{print m+0}')
    { [ -z "$rec_seq" ] || [ "$rec_seq" = "0" ]; } && rec_seq="NA"
    if [ "$rec_seq" != "NA" ] && [ "${reached:-0}" -gt 0 ]; then
      if [ "$rec_seq" -ge "$reached" ]; then
        integ="ok+$((rec_seq - reached))"
      else
        integ="LOSS$((rec_seq - reached))"
      fi
    fi
    log "[4/4] reopen probe (readonly)"
    local vf_t vf_rc
    vf_t=$(time_cmd "$outdir/verify.stdout" "$outdir/verify.stderr" \
      "$DB_BENCH" $cmn \
        --benchmarks="${prefix}readseq,sstables" \
        --reads=200000000 \
        --threads=1 \
        --readonly=1 \
        --use_existing_db=1)
    vf_rc=$?
    if [ "$vf_rc" -eq 0 ]; then reopen="ok"; else reopen="fail"; fi
    log "  integrity: recovered_seq=$rec_seq reached_ops=$reached -> $integ ; reopen=$reopen (${vf_t}s)"
  fi

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$mode" "0" "0" "$wl_rc" "$wl_t" "$TARGET_OPS" "$reached" "$rc_rc" "$rc_t" \
    "$dbopen_ms" "$wal_ms" "$wal_n" "$rc_status" "$rec_seq" "$integ" "$reopen" "$wal_read_mb" "$wal_read_ios" >>"$SUMMARY"
}

log "=== Crash-recovery test ==="
log "DEVICE=$DEVICE TESTTYPE=$TESTTYPE MODES='$MODES'"
log "TARGET_OPS=$TARGET_OPS THREADS=$THREADS POLL=${STATS_POLL_S}s"
log "Results: $RESULT_DIR  Summary: $SUMMARY"

SECONDS=0
for mode in $MODES; do
  run_mode "$mode" || true
done
log
log "All modes done. Total: $(printf '%dh %02dm %02ds' $((SECONDS/3600)) $(((SECONDS%3600)/60)) $((SECONDS%60)))"
log "Summary table:"
{
  echo
  column -t -s $'\t' "$SUMMARY"
  echo
} | tee -a "$LOG"
