#!/usr/bin/env bash
# Usage: bash scripts/make_sample.sh customer_shopping_data.csv
# Creates a 1,000-row sample (with header) from the raw CSV.
# Output: data/samples/customer_shopping_data.sample.csv

DATAFILE="${1:-customer_shopping_data.csv}"

mkdir -p data/samples

(head -n 1 "$DATAFILE" && tail -n +2 "$DATAFILE" | shuf -n 1000 --random-source=/dev/urandom) \
  > data/samples/customer_shopping_data.sample.csv
