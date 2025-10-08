#!/bin/bash

# Pathing from repo
EDGE_FILE="../data/edges_thresholded.tsv"
CUSTOMER_DATA_CSV="../data/samples/customer_shopping_data.sample.csv"
OUTPUT_FILE="../out/cluster_outcomes.tsv"

# Temporary files
CUSTOMER_DATA_TSV="customer_data_sorted.tsv"
EDGES_SORTED="edges_thresholded_sorted.tsv"
JOINED_TOGETHER="joined_together.tsv"
LEFT_OUTCOME="left_outcome.tsv"
LEFT_OUTCOME_SORTED="left_outcome_sorted.tsv"

# Step 1: Prepare customer data: remove header, convert to TSV, sort by customer_id (col 2)
tail -n +2 "$CUSTOMER_DATA_CSV" | tr ',' '\t' | sort -k2,2 > "$CUSTOMER_DATA_TSV"

# Step 2: Sort edges by right entity (customer_id, col 2)
sort -k2,2 "$EDGE_FILE" > "$EDGES_SORTED"

# Step 3: Join on customer_id (col 2 in both files)
join -1 2 -2 2 -t $'\t' "$EDGES_SORTED" "$CUSTOMER_DATA_TSV" > "$JOINED_TOGETHER"

# Step 4: Extract LeftEntity (category, col 2 in joined_together) and price (col 8 in joined_together)
cut -f2,8 "$JOINED_TOGETHER" > "$LEFT_OUTCOME"

# Step 5: Sort by LeftEntity
sort -k1,1 "$LEFT_OUTCOME" > "$LEFT_OUTCOME_SORTED"

# Step 6: Run datamash to compute count, mean, median of price grouped by LeftEntity
datamash -g 1 count 2 mean 2 median 2 < "$LEFT_OUTCOME_SORTED" > "$OUTPUT_FILE"

echo "The summary was saved to $OUTPUT_FILE"

rm customer_data_sorted.tsv edges_thresholded_sorted.tsv
rm joined_together.tsv left_outcome.tsv left_outcome_sorted.tsv

