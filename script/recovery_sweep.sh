#!/bin/bash
# Repeat the recovery test N times with TARGET_OPS drawn uniformly from [LO, HI],
# then aggregate the WAL replay times per mode.
#
# Usage: sudo -E bash script/recovery_sweep.sh
# Env: N (100), TARGET_OPS_LO (1000000), TARGET_OPS_HI (8000000), DEVICE (samsung),
#      MODES ("append zrwa"), BASE (script/results/<ts>_recovery_sweep)

set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
DEVICE=${DEVICE:-samsung}
MODES=${MODES:-"append zrwa"}
N=${N:-100}
LO=${TARGET_OPS_LO:-1000000}
HI=${TARGET_OPS_HI:-8000000}
BASE=${BASE:-$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)_recovery_sweep}
mkdir -p "$BASE"
ALL="$BASE/all_runs.tsv"
LOGF="$BASE/repro.log"
log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOGF"; }

log "recovery sweep x$N: TARGET_OPS in [$LO,$HI], modes='$MODES', device=$DEVICE"
log "db_bench: $(stat -c '%y' "$ROOT/db_bench")"
log "git HEAD: $(git -C "$ROOT" rev-parse --short HEAD)"

for i in $(seq 1 "$N"); do
  t=$(shuf -i "$LO"-"$HI" -n 1)
  ITER="$BASE/iter_$(printf %03d "$i")"
  log "=== iter $i/$N TARGET_OPS=$t -> $ITER"
  TARGET_OPS=$t RESULT_DIR="$ITER" \
    bash "$SCRIPT_DIR/recovery_test.sh" "$DEVICE" 0 $MODES \
    >>"$LOGF" 2>&1
  rc=$?
  [ $rc -ne 0 ] && log "WARN iter $i exit=$rc"
  if [ -f "$ITER/summary.tsv" ]; then
    if [ ! -f "$ALL" ]; then
      { printf "iter\t"; head -1 "$ITER/summary.tsv"; } >"$ALL"
    fi
    tail -n +2 "$ITER/summary.tsv" | while IFS= read -r row; do
      printf "%s\t%s\n" "$i" "$row" >>"$ALL"
    done
    tail -n +2 "$ITER/summary.tsv" | tee -a "$LOGF"
  fi
done

# cols (with iter prefix): 2 mode, 6 workload_secs, 11 dbopen, 12 replay,
# 14 status, 15 recovered_seq, 16 integrity, 17 reopen
awk -F'\t' 'NR>1 && $14=="ok" {
    m=$2; v=$12+0; n[m]++; s[m]+=v; s2[m]+=v*v
    if (!(m in mn) || v<mn[m]) mn[m]=v
    if (v>mx[m]) mx[m]=v
    dbo[m]+=$11+0; wl[m]+=$6+0
    if ($16 ~ /^LOSS/) loss[m]++
    if ($17 == "fail") rfail[m]++
  }
  NR>1 && $14!="ok" { bad[$2]++ }
  END {
    printf "mode\tn_ok\tn_bad\twal_replay_ms_mean\tsd\tmin\tmax\tdbopen_ms_mean\tworkload_s_mean\tintegrity_loss\treopen_fail\n"
    for (m in n) {
      mean=s[m]/n[m]
      sd=(n[m]>1) ? sqrt((s2[m]-s[m]*s[m]/n[m])/(n[m]-1)) : 0
      printf "%s\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%d\n", \
        m, n[m], bad[m]+0, mean, sd, mn[m], mx[m], dbo[m]/n[m], wl[m]/n[m], loss[m]+0, rfail[m]+0
    }
  }' "$ALL" | tee "$BASE/aggregate.tsv" | tee -a "$LOGF"
log "DONE base=$BASE"
