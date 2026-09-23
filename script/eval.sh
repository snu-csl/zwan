#!/bin/bash
# Usage: ./eval.sh DEVICE [PHASE] [START_IDX] [MODE]
#   DEVICE samsung|wd, PHASE microbench|mixgraph|sens-threads|sens-valuesize|all

set -euo pipefail

DEVICE=${1:?"Usage: $0 <samsung|wd> [PHASE] [START_IDX] [MODE]"}
PHASE=${2:-all}
START_IDX=${3:-0}
MODE_FILTER=${4:-all}

if [[ "$DEVICE" != "samsung" && "$DEVICE" != "wd" ]]; then
  echo "Unknown device: $DEVICE (available: samsung wd)" >&2
  exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
RUN_SCRIPT="$SCRIPT_DIR/bench.sh"
ROCKSDB_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

VSIZES="100 500 1000 2000 4000"
SCALE=${SCALE:-1}
PREFILL_KEYS=$((64000000 / SCALE))
RUN_KEYS=$((20000000 / SCALE))
MIXGRAPH_PREFILL_KEYS=$((64000000 / SCALE))
MIXGRAPH_RUN_KEYS=$((20000000 / SCALE))
RUN_DURATION_SECONDS=${DURATION:-600}
THREADS=4
MODES="zenfs zenfs_nm append zrwa"
RESULT_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)_nvmsa_${DEVICE}_${PHASE}"
SUMMARY_FILE="$RESULT_DIR/summary.tsv"

ulimit -n 100000

print_duration() {
  local secs=$1
  local hrs=$((secs / 3600))
  secs=$((secs % 3600))
  local mins=$((secs / 60))
  secs=$((secs % 60))
  echo "${hrs}h ${mins}m ${secs}s"
}

drop_cache() {
  sync
  echo 3 >/proc/sys/vm/drop_caches
}

format_zns() {
  local wal_mode=${1:-append}
  rm -f /var/tmp/spdk_pci_lock*
  cd "$ROCKSDB_ROOT"
  ./format_mkfs.sh "$DEVICE" "$wal_mode"
  cd "$SCRIPT_DIR"
}

log() {
  echo "[$(date '+%H:%M:%S')] $*" | tee -a "$RESULT_DIR/eval.log"
}

mode_to_testmode() {
  case "$1" in
    zenfs|zenfs_nm) echo 1 ;;
    append|zrwa) echo 0 ;;
    *) echo "unknown mode: $1" >&2; exit 1 ;;
  esac
}

mode_to_walmode() {
  case "$1" in
    zenfs|zenfs_nm) echo append ;;
    append) echo append ;;
    zrwa) echo zrwa ;;
    *) echo "unknown mode: $1" >&2; exit 1 ;;
  esac
}

mode_to_prefix() {
  case "$1" in
    zenfs) echo zenfs ;;
    zenfs_nm) echo zenfs_nm ;;
    append|zrwa) echo waltz ;;
    *) echo "unknown mode: $1" >&2; exit 1 ;;
  esac
}

mode_to_extra_opts() {
  local opts=""
  case "$1" in
    zenfs_nm) opts="--zenfs_skip_meta_sync=true" ;;
  esac
  if [[ "$1" == "zrwa" && "$DEVICE" == "wd" ]]; then
    opts="$opts --zrwa_exp_flush=false"
  fi
  echo "$opts"
}

phase_to_subdir() {
  case "$1" in
    microbench) echo microbench ;;
    mixgraph) echo mixgraph ;;
    sens-threads) echo sens_threads ;;
    sens-valuesize) echo sens_valuesize ;;
    *) echo "unknown phase: $1" >&2; exit 1 ;;
  esac
}

resolve_modes() {
  case "$MODE_FILTER" in
    all)
      echo "$MODES"
      ;;
    zenfs|zenfs_nm|append|zrwa)
      echo "$MODE_FILTER"
      ;;
    *)
      echo "unknown mode filter: $MODE_FILTER" >&2
      exit 1
      ;;
  esac
}

generate_microbench_experiments() {
  local testtype vsize mode
  local modes
  modes=$(resolve_modes)
  for testtype in 0 1 2 3 4 5; do
    for vsize in $VSIZES; do
      for mode in $modes; do
        printf "%s %s %s\n" "$testtype" "$vsize" "$mode"
      done
    done
  done
}

generate_mixgraph_experiments() {
  local mode testtype
  local modes
  modes=$(resolve_modes)
  # testtype 6 to 9: AllRand, AllDist, PreRand, PreDist mixgraph workloads
  for testtype in 6 7 8 9; do
    for mode in $modes; do
      printf "%s %s\n" "$testtype" "$mode"
    done
  done
}

generate_sens_threads_experiments() {
  local threads vsize mode
  local modes
  modes=$(resolve_modes)
  for threads in 1 2 4 8; do
    for vsize in $VSIZES; do
      for mode in $modes; do
        printf "%s %s %s\n" "$threads" "$vsize" "$mode"
      done
    done
  done
}

generate_sens_valuesize_experiments() {
  local vsize mode
  local modes
  modes=$(resolve_modes)
  for vsize in $VSIZES; do
    for mode in $modes; do
      printf "%s %s\n" "$vsize" "$mode"
    done
  done
}

generate_phase_experiments() {
  case "$1" in
    microbench) generate_microbench_experiments ;;
    mixgraph) generate_mixgraph_experiments ;;
    sens-threads) generate_sens_threads_experiments ;;
    sens-valuesize) generate_sens_valuesize_experiments ;;
    *) echo "unknown phase: $1" >&2; exit 1 ;;
  esac
}

phase_count() {
  generate_phase_experiments "$1" | wc -l | tr -d ' '
}

extract_waf_rows() {
  local stderr_log=$1
  awk '
    function reset_block() {
      mode = ""
      phase = ""
      user_data = ""
      wal_payload = ""
      wal_record = ""
      wal_device = ""
      total_device = ""
      meta_device = ""
      wal_waf = ""
      total_waf = ""
      meta_waf = ""
    }

    BEGIN {
      in_block = 0
      reset_block()
    }

    /^===== WAF Stats =====$/ {
      in_block = 1
      reset_block()
      next
    }

    /^=====================$/ {
      if (in_block && phase != "") {
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
               phase, mode, user_data, wal_payload, wal_record,
               wal_device, total_device, meta_device,
               wal_waf, total_waf, meta_waf
      }
      in_block = 0
      next
    }

    !in_block { next }

    /^Mode: / {
      sub(/^Mode: /, "", $0)
      mode = $0
      next
    }

    /^Phase: / {
      sub(/^Phase: /, "", $0)
      phase = $0
      next
    }

    /^User data bytes: / {
      sub(/^User data bytes: /, "", $0)
      user_data = $0
      next
    }

    /^WAL payload bytes: / {
      sub(/^WAL payload bytes: /, "", $0)
      wal_payload = $0
      next
    }

    /^WAL record count: / {
      sub(/^WAL record count: /, "", $0)
      wal_record = $0
      next
    }

    /^WAL device bytes: / {
      sub(/^WAL device bytes: /, "", $0)
      wal_device = $0
      next
    }

    /^Total device bytes: / {
      sub(/^Total device bytes: /, "", $0)
      total_device = $0
      next
    }

    /^Meta device bytes: / {
      sub(/^Meta device bytes: /, "", $0)
      meta_device = $0
      next
    }

    /^WAL WAF: / {
      sub(/^WAL WAF: /, "", $0)
      wal_waf = $0
      next
    }

    /^Total WAF: / {
      sub(/^Total WAF: /, "", $0)
      total_waf = $0
      next
    }

    /^Meta WAF: / {
      sub(/^Meta WAF: /, "", $0)
      meta_waf = $0
      next
    }
  ' "$stderr_log"
}

append_summary_rows() {
  local subdir=$1
  local mode=$2
  local testtype=$3
  local vsize=$4
  local krange=$5
  local ktest=$6
  local threads=$7
  local label=$8
  local exit_code=$9
  local stdout_log=${10}
  local stderr_log=${11}

  local phase
  declare -A phase_rows=()

  if [ -f "$stderr_log" ]; then
    while IFS=$'\t' read -r phase reported_mode user_data wal_payload wal_record wal_device total_device meta_device wal_waf total_waf meta_waf; do
      [ -n "$phase" ] || continue
      phase_rows["$phase"]="${reported_mode}"$'\t'"${user_data}"$'\t'"${wal_payload}"$'\t'"${wal_record}"$'\t'"${wal_device}"$'\t'"${total_device}"$'\t'"${meta_device}"$'\t'"${wal_waf}"$'\t'"${total_waf}"$'\t'"${meta_waf}"
    done < <(extract_waf_rows "$stderr_log")
  fi

  for phase in prefill workload; do
    local reported_mode="NA"
    local user_data="NA"
    local wal_payload="NA"
    local wal_record="NA"
    local wal_device="NA"
    local total_device="NA"
    local meta_device="NA"
    local wal_waf="NA"
    local total_waf="NA"
    local meta_waf="NA"
    local status
    local mode_check="NA"

    if [ -n "${phase_rows[$phase]+x}" ]; then
      IFS=$'\t' read -r reported_mode user_data wal_payload wal_record wal_device total_device meta_device wal_waf total_waf meta_waf <<<"${phase_rows[$phase]}"
      if [ "$exit_code" -eq 0 ]; then
        status="ok"
      else
        status="failed"
      fi
      if [ "$reported_mode" = "$mode" ]; then
        mode_check="ok"
      else
        mode_check="mismatch"
      fi
    else
      if [ "$exit_code" -eq 0 ]; then
        status="missing_waf"
      else
        status="failed"
      fi
    fi

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$phase" "$subdir" "$label" "$mode" "$reported_mode" "$mode_check" \
      "$testtype" "$vsize" "$krange" "$ktest" "$threads" "$exit_code" "$status" \
      "$user_data" "$wal_payload" "$wal_record" "$wal_device" "$total_device" "$meta_device" \
      "$wal_waf" "$total_waf" "$meta_waf" \
      "$stderr_log" "$stdout_log" >>"$SUMMARY_FILE"
  done
}

run_one() {
  local subdir=$1
  local mode=$2
  local testtype=$3
  local vsize=$4
  local krange=$5
  local ktest=$6
  local threads=$7
  local label=$8

  local outdir="$RESULT_DIR/$subdir"
  local testmode
  local walmode
  local prefix
  local src_stdout
  local src_stderr
  local dst_stdout="$outdir/stdout_${label}.log"
  local dst_stderr="$outdir/stderr_${label}.log"
  local bench_rc

  mkdir -p "$outdir"
  testmode=$(mode_to_testmode "$mode")
  walmode=$(mode_to_walmode "$mode")
  prefix=$(mode_to_prefix "$mode")
  local extra_opts
  extra_opts=$(mode_to_extra_opts "$mode")

  log "=== [$((cur_idx + 1))/$total_exps] $label ==="
  log "  format_zns start"
  SECONDS=0
  format_zns "$walmode"
  local fmt_time=$SECONDS
  log "  format_zns done ($(print_duration "$fmt_time"))"

  drop_cache

  log "  bench start: mode=$mode testmode=$testmode walmode=$walmode type=$testtype vsize=$vsize krange=$krange ktest=$ktest threads=$threads duration=${RUN_DURATION_SECONDS}s"
  SECONDS=0
  cd "$SCRIPT_DIR"
  set +e
  if [ "$testtype" -gt 5 ]; then
    MIXGRAPH_SKIP_VALUESIZE_ARGS=1 \
    MIXGRAPH_SKIP_DIST_ARGS=1 \
    RUN_DURATION_SECONDS=$RUN_DURATION_SECONDS \
    EXTRA_BENCH_OPTS="$extra_opts" \
    BENCH_OUT_PREFIX="$prefix" \
      "$RUN_SCRIPT" "$testmode" "$testtype" "$vsize" "$krange" "$ktest" "$walmode" "$threads" "$DEVICE"
  else
    RUN_DURATION_SECONDS=$RUN_DURATION_SECONDS \
    EXTRA_BENCH_OPTS="$extra_opts" \
    BENCH_OUT_PREFIX="$prefix" \
      "$RUN_SCRIPT" "$testmode" "$testtype" "$vsize" "$krange" "$ktest" "$walmode" "$threads" "$DEVICE"
  fi
  bench_rc=$?
  set -e
  local bench_time=$SECONDS
  log "  bench done ($(print_duration "$bench_time")) exit_code=$bench_rc"

  src_stdout="$SCRIPT_DIR/stdout_${prefix}_test${testtype}.log"
  src_stderr="$SCRIPT_DIR/stderr_${prefix}_test${testtype}.log"

  if [ -f "$src_stdout" ]; then
    mv "$src_stdout" "$dst_stdout"
  else
    : >"$dst_stdout"
  fi

  if [ -f "$src_stderr" ]; then
    mv "$src_stderr" "$dst_stderr"
  else
    : >"$dst_stderr"
  fi

  append_summary_rows "$subdir" "$mode" "$testtype" "$vsize" "$krange" "$ktest" \
    "$threads" "$label" "$bench_rc" "$dst_stdout" "$dst_stderr"

  echo "$label | fmt=$(print_duration "$fmt_time") bench=$(print_duration "$bench_time") exit=$bench_rc" >>"$RESULT_DIR/time.log"
  log "  total ($(print_duration $((fmt_time + bench_time))))"
}

run_phase() {
  local phase_name=$1
  local subdir
  local record

  subdir=$(phase_to_subdir "$phase_name")
  total_exps=$(phase_count "$phase_name")
  cur_idx=0

  log "Phase: $phase_name ($total_exps experiments)"

  while read -r record; do
    [ -n "$record" ] || continue

    if [ "$cur_idx" -lt "$START_IDX" ]; then
      cur_idx=$((cur_idx + 1))
      continue
    fi

    case "$phase_name" in
      microbench)
        local testtype vsize mode krange ktest label
        read -r testtype vsize mode <<<"$record"
        krange=$PREFILL_KEYS
        ktest=$RUN_KEYS
        label="${mode}_v${vsize}_test${testtype}"
        run_one "$subdir" "$mode" "$testtype" "$vsize" "$krange" "$ktest" "$THREADS" "$label"
        ;;
      mixgraph)
        local testtype mode krange ktest label
        read -r testtype mode <<<"$record"
        krange=$MIXGRAPH_PREFILL_KEYS
        ktest=$MIXGRAPH_RUN_KEYS
        label="${mode}_test${testtype}"
        # vsize is only a log label here, testtype > 5 ignores --value_size
        run_one "$subdir" "$mode" "$testtype" "default" "$krange" "$ktest" "$THREADS" "$label"
        ;;
      sens-threads)
        local threads vsize mode krange ktest label
        read -r threads vsize mode <<<"$record"
        krange=$PREFILL_KEYS
        ktest=$RUN_KEYS
        label="${mode}_v${vsize}_t${threads}_test9"
        run_one "$subdir" "$mode" 9 "$vsize" "$krange" "$ktest" "$threads" "$label"
        ;;
      sens-valuesize)
        local vsize mode krange ktest label
        read -r vsize mode <<<"$record"
        krange=$PREFILL_KEYS
        ktest=$RUN_KEYS
        label="${mode}_v${vsize}_test9"
        run_one "$subdir" "$mode" 9 "$vsize" "$krange" "$ktest" "$THREADS" "$label"
        ;;
    esac

    cur_idx=$((cur_idx + 1))
  done < <(generate_phase_experiments "$phase_name")
}

run_all_phases() {
  local start_all=$START_IDX
  local phase_name
  local count

  for phase_name in microbench mixgraph sens-threads sens-valuesize; do
    count=$(phase_count "$phase_name")
    if [ "$start_all" -ge "$count" ]; then
      start_all=$((start_all - count))
      continue
    fi
    START_IDX=$start_all
    run_phase "$phase_name"
    start_all=0
  done
}

log_summary_counts() {
  local missing_rows
  local failed_rows
  local mismatch_rows

  missing_rows=$(awk -F'\t' 'NR > 1 && $13 == "missing_waf" {count++} END {print count + 0}' "$SUMMARY_FILE")
  failed_rows=$(awk -F'\t' 'NR > 1 && $13 == "failed" {count++} END {print count + 0}' "$SUMMARY_FILE")
  mismatch_rows=$(awk -F'\t' 'NR > 1 && $6 == "mismatch" {count++} END {print count + 0}' "$SUMMARY_FILE")

  log "Summary rows: missing_waf=$missing_rows failed=$failed_rows mode_mismatch=$mismatch_rows"
}

mkdir -p "$RESULT_DIR"
printf "phase\tsubdir\tlabel\tmode\treported_mode\tmode_check\ttesttype\tvalue_size\tkeyrange\tkeytest\tthreads\texit_code\tstatus\tuser_data_bytes\twal_payload_bytes\twal_record_count\twal_device_bytes\ttotal_device_bytes\tmeta_device_bytes\twal_waf\ttotal_waf\tmeta_waf\tstderr_log\tstdout_log\n" >"$SUMMARY_FILE"

log "eval.sh started: DEVICE=$DEVICE PHASE=$PHASE START_IDX=$START_IDX MODE=$MODE_FILTER"
log "Results: $RESULT_DIR"
case "$PHASE" in
  mixgraph)
    log "Config: prefill=$MIXGRAPH_PREFILL_KEYS run=$MIXGRAPH_RUN_KEYS threads=$THREADS run_duration=${RUN_DURATION_SECONDS}s value-size scaling=disabled"
    ;;
  all)
    log "Config (microbench/sens): prefill=$PREFILL_KEYS run=$RUN_KEYS threads=$THREADS run_duration=${RUN_DURATION_SECONDS}s"
    log "Config (mixgraph):        prefill=$MIXGRAPH_PREFILL_KEYS run=$MIXGRAPH_RUN_KEYS threads=$THREADS run_duration=${RUN_DURATION_SECONDS}s"
    ;;
  *)
    log "Config: prefill=$PREFILL_KEYS run=$RUN_KEYS threads=$THREADS run_duration=${RUN_DURATION_SECONDS}s value-size scaling=disabled"
    ;;
esac

SECONDS=0

case "$PHASE" in
  microbench|mixgraph|sens-threads|sens-valuesize)
    run_phase "$PHASE"
    ;;
  all)
    run_all_phases
    ;;
  *)
    echo "Usage: $0 <samsung|wd> {microbench|mixgraph|sens-threads|sens-valuesize|all} [START_IDX] [zenfs|zenfs_nm|append|zrwa]" >&2
    exit 1
    ;;
esac

log_summary_counts
log "All done. Total time: $(print_duration "$SECONDS")"
