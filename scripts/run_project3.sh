# NOTE: Before running this script, please ensure the following are installed:
# - gnuplot (for histogram plotting)
# - datamash (for summary statistics)
# - Python 3 with networkx and matplotlib (for network and outcome plots)
# Install with:
#   sudo apt-get update && sudo apt-get install gnuplot datamash
#   pip3 install networkx matplotlib
#!/bin/bash
# cluster_outcomes_script.sh
# End-to-end pipeline for Istanbul Shopping Malls clustering assignment
set -e

# Directories and files
DATA_DIR="data/Shopping Mall Data in Istanbul"
SAMPLE_CSV="$DATA_DIR/customer_shopping_data.csv"
EDGE_FILE="edges.tsv"
EDGE_THRESH_FILE="edges_thresholded.tsv"
ENTITY_COUNTS_FILE="entity_counts.tsv"
CLUSTER_SIZES_FILE="cluster_sizes.tsv"
CLUSTER_HIST_FILE="cluster_histogram.png"
TOP30_CLUSTERS_FILE="top30_clusters.txt"
TOP30_OVERALL_FILE="top30_overall.txt"
DIFF_TOP30_FILE="diff_top30.txt"
CLUSTER_VIZ_FILE="cluster_viz.png"
LEFT_OUTCOME_FILE="left_outcome.tsv"
CLUSTER_OUTCOMES_FILE="cluster_outcomes.tsv"

# Step 1: Define relationships (edges)
echo "Step 1: Extracting edges (category, customer_id)"
awk -F',' 'NR>1 {print $5"\t"$2}' "$SAMPLE_CSV" | sort > "$EDGE_FILE"

# Step 2: Filter significant clusters (frequency >= N)
echo "Step 2: Filtering clusters by frequency"
N=10
cut -f1 "$EDGE_FILE" | sort | uniq -c | awk -v n="$N" '$1>=n {print $2"\t"$1}' | sort > "$ENTITY_COUNTS_FILE"
awk 'NR==FNR{a[$1];next} $1 in a' "$ENTITY_COUNTS_FILE" "$EDGE_FILE" > "$EDGE_THRESH_FILE"

# Step 3: Histogram of cluster sizes
echo "Step 3: Computing cluster sizes and plotting histogram"
cut -f1 "$EDGE_THRESH_FILE" | sort | uniq -c | awk '{print $2"\t"$1}' | sort > "$CLUSTER_SIZES_FILE"
gnuplot <<EOF
set terminal png size 1200,400
set output '$CLUSTER_HIST_FILE'
set style data histograms
set style fill solid 1.0 border -1
set boxwidth 0.5
set xlabel "Cluster Size (# of edges)"
set ylabel "Number of Clusters"
set xtic rotate by -45
plot "$CLUSTER_SIZES_FILE" using 2:xtic(1) title "Cluster Sizes"
EOF

# Step 4: Top-30 tokens inside clusters
echo "Step 4: Top-30 tokens in clusters and overall"
# Use category (column 5) for top tokens
awk -F'\t' '{print $1}' "$EDGE_THRESH_FILE" | sort | uniq -c | sort -nr | head -30 > "$TOP30_CLUSTERS_FILE"
awk -F'\t' '{print $1}' "$EDGE_FILE" | sort | uniq -c | sort -nr | head -30 > "$TOP30_OVERALL_FILE"
diff "$TOP30_CLUSTERS_FILE" "$TOP30_OVERALL_FILE" > "$DIFF_TOP30_FILE"

# Step 5: Network visualization (NetworkX example)
echo "Step 5: Network visualization"
if [ -f scripts/plot_network.py ]; then
    python3 scripts/plot_network.py "$EDGE_THRESH_FILE" "$CLUSTER_VIZ_FILE"
else
    echo "plot_network.py not found. Please create a script to visualize the network."
fi

# Step 6: Summary statistics about clusters
echo "Step 6: Summary statistics with datamash"
awk -F',' 'NR>1 {gsub(/^[ \t]+|[ \t]+$/, "", $7); if ($7 ~ /^[0-9]+(\.[0-9]+)?$/) print $5"\t"$7}' "$SAMPLE_CSV" | sort > "$LEFT_OUTCOME_FILE"
awk '{if ($2 ~ /^[0-9]+(\.[0-9]+)?$/) print $1"\t"$2}' "$LEFT_OUTCOME_FILE" | sort | datamash -g 1 count 2 mean 2 median 2 > "$CLUSTER_OUTCOMES_FILE"

# Optional: plot summary statistics
if [ -f scripts/plot_cluster_outcomes.py ]; then
    python3 scripts/plot_cluster_outcomes.py "$CLUSTER_OUTCOMES_FILE" cluster_outcomes_plot.png
else
    echo "plot_cluster_outcomes.py not found. Skipping summary plot."
fi



# Move all deliverables to output folder
OUT_DIR="out_project3"
mkdir -p "$OUT_DIR"
mv "$EDGE_FILE" "$OUT_DIR/edges.tsv"
mv "$EDGE_THRESH_FILE" "$OUT_DIR/edges_thresholded.tsv"
mv "$ENTITY_COUNTS_FILE" "$OUT_DIR/entity_counts.tsv"
mv "$CLUSTER_SIZES_FILE" "$OUT_DIR/cluster_sizes.tsv"
mv "$CLUSTER_HIST_FILE" "$OUT_DIR/cluster_histogram.png"
mv "$TOP30_CLUSTERS_FILE" "$OUT_DIR/top30_clusters.txt"
mv "$TOP30_OVERALL_FILE" "$OUT_DIR/top30_overall.txt"
mv "$DIFF_TOP30_FILE" "$OUT_DIR/diff_top30.txt"
mv "$CLUSTER_VIZ_FILE" "$OUT_DIR/cluster_viz.png"
mv "$LEFT_OUTCOME_FILE" "$OUT_DIR/left_outcome.tsv"
mv "$CLUSTER_OUTCOMES_FILE" "$OUT_DIR/cluster_outcomes.tsv"
if [ -f cluster_outcomes_plot.png ]; then
    mv cluster_outcomes_plot.png "$OUT_DIR/cluster_outcomes_plot.png"
fi

echo "All outputs moved to $OUT_DIR/ for project submission."
echo "Pipeline complete. Outputs generated."

