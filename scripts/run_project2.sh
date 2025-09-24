#!/usr/bin/env bash
# Usage: bash scripts/run_project2.sh /full/path/to/customer_shopping_data.csv
# Assumptions: delimiter=",", header in first row, encoding UTF-8/us-ascii.
# This entry script:
#  - (B) creates a 1,000-row sample with header -> data/samples/
#  - (C) build frequency tables (write to out/)
#  - (D) compute Top-N summaries (write to out/)
#  - emits a run log -> out/run.log

set -euo pipefail

DATAFILE="${1:-}"
if [[ -z "${DATAFILE}" || ! -f "${DATAFILE}" ]]; then
  echo "ERROR: provide path to raw CSV, e.g.:"
  echo "  bash scripts/run_project2.sh \"data/Shopping Mall Data in Istanbul/customer_shopping_data.csv\""
  exit 1
fi

mkdir -p data/samples out
LOG="out/run.log"
: > "$LOG"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

{
  echo "=== Run started: $(ts) ==="
  echo "Input: $DATAFILE"
  echo "Delimiter: ',' | Header: yes | Encoding: UTF-8/us-ascii"

  echo "[B] Sampling 1,000 rows with header -> data/samples/customer_shopping_data.sample.csv"
  (head -n 1 "$DATAFILE" && tail -n +2 "$DATAFILE" | shuf -n 1000 --random-source=/dev/urandom) \
    > data/samples/customer_shopping_data.sample.csv



  echo "[C] frequency tables (e.g., category, payment_method) -> out/freq_*.txt"
  cut -d, -f5 "$DATAFILE" | tail -n +2 | sort | uniq -c | sort -nr | tee out/freq_category.txt | head -n 10

  cut -d, -f8 "$DATAFILE" | tail -n +2 | sort | uniq -c | sort -nr | tee out/freq_payment_method.txt | head -n 10

  echo "[D] Top-N (e.g., top malls, top spenders) -> out/topN_*.csv"
  cut -d, -f4 "$DATAFILE" | tail -n +2 | sort | uniq -c | sort -nr | head -n 5 | tee out/top5_ages.txt

  cut -d, -f10 "$DATAFILE" | tail -n +2 | sort | uniq -c | sort -nr | head -n 5 | tee out/top5_malls.txt

  { echo "customer_id,gender,age"
    cut -d, -f2,3,4 "$DATAFILE" | tail -n +2 | sort -u
  } | tee out/skinny_customer_demo.csv  | head -n 5

  { echo "File stats"
    wc "$DATAFILE"
    echo "Header"
    head -n 1 "$DATAFILE"
  } > out/run_summary.txt \
    2> out/run_errors.txt

  cut -d, -f2 "$DATAFILE" | tail -n +2 | sort | uniq -c \
    > out/customer_counts.spc

  sort -k2,2 out/customer_counts.spc \
    -o out/customer_counts.spc

  cut -d, -f2 "$DATAFILE" | tail -n +2 > out/ids.tmp
  cut -d, -f3 "$DATAFILE" | tail -n +2 > out/genders.tmp
  paste out/ids.tmp \
        out/genders.tmp \
  | sort -k1,1 -u \
  > out/id_gender.spc

  join -1 2 -2 1 \
    out/customer_counts.spc \
    out/id_gender.spc \
  | sort -nr \
  | tee out/top_customers_with_gender.txt \
  | head -n 10

  rm -f out/ids.tmp \
        out/genders.tmp


  echo "=== Run finished: $(ts) ==="
} | tee -a "$LOG"
