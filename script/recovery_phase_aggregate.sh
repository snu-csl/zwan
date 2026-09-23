#!/bin/bash
# Aggregate the WAL replay breakdown lines of a recovery result tree.
# Usage: recovery_phase_aggregate.sh <BASE_DIR> [modes...]
# Reads <BASE_DIR>/iter_*/<mode>/recover.stderr.

set -u
BASE_DIR=$1
shift || true
MODES=${*:-"zrwa append zenfs"}

OUT="$BASE_DIR/wal_replay_phases.tsv"
{
  echo -e "mode\tn\textent_recover_us_mean\textent_recover_us_sd\textent_recover_us_min\textent_recover_us_max\tread_us_mean\tread_us_sd\tread_us_min\tread_us_max\tmemtable_insert_us_mean\tl0_flush_us_mean\tother_us_mean\ttotal_us_mean"
  for mode in $MODES; do
    files=$(ls "$BASE_DIR"/iter_*/$mode/recover.stderr 2>/dev/null || true)
    if [ -z "$files" ]; then
      printf "%s\t0\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\n" "$mode"
      continue
    fi
    awk -v mode="$mode" '
      /\[WAL_REPLAY\]/ {
        gsub(/=/, " ")
        p6=p7a=p7b=p7c=po=ft=0
        for (i=1; i<=NF; i++) {
          if ($i == "extent_recover_us") p6 = $(i+1) + 0
          else if ($i == "read_us") p7a = $(i+1) + 0
          else if ($i == "memtable_insert_us") p7b = $(i+1) + 0
          else if ($i == "l0_flush_us") p7c = $(i+1) + 0
          else if ($i == "other_us") po = $(i+1) + 0
          else if ($i == "total_us") ft = $(i+1) + 0
        }
        n++
        s6 += p6;  s6_2 += p6*p6
        if (n==1 || p6 < min6) min6 = p6
        if (p6 > max6) max6 = p6
        s7a += p7a; s7a_2 += p7a*p7a
        if (n==1 || p7a < min7a) min7a = p7a
        if (p7a > max7a) max7a = p7a
        s7b += p7b; s7c += p7c; sother += po; sft += ft
      }
      END {
        if (n == 0) {
          printf "%s\t0\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\n", mode
          exit
        }
        m6 = s6/n; v6 = s6_2/n - m6*m6; if (v6<0) v6=0; sd6 = sqrt(v6)
        m7a = s7a/n; v7a = s7a_2/n - m7a*m7a; if (v7a<0) v7a=0; sd7a = sqrt(v7a)
        printf "%s\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n",
          mode, n,
          m6, sd6, min6, max6,
          m7a, sd7a, min7a, max7a,
          s7b/n, s7c/n, sother/n, sft/n
      }
    ' $files
  done
} | tee "$OUT"

echo ""
echo "Wrote: $OUT"
