# NOTE: Before running this script, please ensure the following are installed:
# - gnuplot (for histogram plotting)
# - datamash (for summary statistics)
# - Python 3 with packages: networkx, matplotlib, pandas (for network and outcome plots)
# Install with:
#   sudo apt-get update && sudo apt-get install gnuplot datamash
#   pip3 install networkx matplotlib pandas
#!/bin/bash
# run_project3.sh
# End-to-end pipeline for Istanbul Shopping Malls clustering assignment
set -e


# Directories and files
DATA_DIR="data/Shopping Mall Data in Istanbul"
DEFAULT_CSV="$DATA_DIR/customer_shopping_data.csv"
INPUT_CSV="${1:-$DEFAULT_CSV}"
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
CLUSTER_OUTCOMES_PNG="cluster_outcomes_plot.png"


# Step 1: Define relationships (edges)
if [ ! -f "$INPUT_CSV" ]; then
  echo "Error: Input CSV file '$INPUT_CSV' not found."
  echo "Usage: $0 [input_csv]"
  exit 1
fi

echo "Step 1: Extracting edges (category, customer_id)"
awk -F',' 'NR>1 {print $5"\t"$2}' "$INPUT_CSV" | sort > "$EDGE_FILE"

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

# Step 5: Network visualization (embedded Python)
echo "Step 5: Network visualization (embedded Python - NetworkX)"
if command -v python3 >/dev/null 2>&1; then
  python3 - "$EDGE_THRESH_FILE" "$CLUSTER_VIZ_FILE" <<'PYNET'
import sys, random
import matplotlib.pyplot as plt

try:
    import networkx as nx
except ImportError:
    print("This step requires networkx. Install with: pip3 install networkx", file=sys.stderr)
    sys.exit(1)

if len(sys.argv) < 3:
    print("Usage: <stdin> <edges_file> <output_png>", file=sys.stderr)
    sys.exit(1)

edges_file = sys.argv[1]
output_png = sys.argv[2]

# Read edges (tab-separated)
with open(edges_file, "r", encoding="utf-8") as f:
    edges = [line.strip().split("\t") for line in f if "\t" in line]

if not edges:
    print("No edges found (ensure TSV with two columns).", file=sys.stderr)
    sys.exit(1)

sample_size = min(500, len(edges))
random.seed(42)
edges_sample = random.sample(edges, sample_size)

G = nx.Graph()
G.add_edges_from(edges_sample)

# Color nodes by whether they appear as left entities
left_entities = set(e[0] for e in edges_sample)
node_colors = ["tab:blue" if n in left_entities else "tab:gray" for n in G.nodes()]

# Labels for up to 50 nodes
labels = {}
for i, node in enumerate(G.nodes()):
    if i < 50:
        labels[node] = node

plt.figure(figsize=(10,10))
pos = nx.spring_layout(G, seed=42)
nx.draw(G, pos, node_size=100, node_color=node_colors, with_labels=False, edge_color="lightgray")
nx.draw_networkx_labels(G, pos, labels, font_size=8)
plt.title(f"Network Visualization (Sample of {sample_size} edges, up to 50 labels)")
plt.tight_layout()
plt.savefig(output_png)
print(f"Network visualization saved to {output_png}")
PYNET
else
  echo "python3 not found. Skipping network visualization."
fi

# Step 6: Summary statistics about clusters
echo "Step 6: Summary statistics with datamash"
awk -F',' 'NR>1 {gsub(/^[ \t]+|[ \t]+$/, "", $7); if ($7 ~ /^[0-9]+(\.[0-9]+)?$/) print $5"\t"$7}' "$INPUT_CSV" | sort > "$LEFT_OUTCOME_FILE"
awk '{if ($2 ~ /^[0-9]+(\.[0-9]+)?$/) print $1"\t"$2}' "$LEFT_OUTCOME_FILE" | sort | datamash -g 1 count 2 mean 2 median 2 > "$CLUSTER_OUTCOMES_FILE"

# Step 7: Plot summary statistics (embedded Python)
echo "Step 7: Plotting cluster outcomes (embedded Python - pandas/matplotlib)"
if command -v python3 >/dev/null 2>&1; then
  python3 - "$CLUSTER_OUTCOMES_FILE" "$CLUSTER_OUTCOMES_PNG" <<'PYOUT'
import sys
import pandas as pd
import matplotlib.pyplot as plt

if len(sys.argv) < 3:
    print("Usage: <stdin> <cluster_outcomes.tsv> <output_png>", file=sys.stderr)
    sys.exit(1)

infile  = sys.argv[1]
outfile = sys.argv[2]

# TSV columns: Entity, Count, Mean, Median
df = pd.read_csv(infile, sep="\t", header=None, names=["Entity", "Count", "Mean", "Median"])

plt.figure(figsize=(10,6))
plt.bar(df["Entity"], df["Mean"])
plt.xlabel("Entity")
plt.ylabel("Mean Outcome")
plt.title("Mean Outcome by Cluster Entity")
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(outfile)
print(f"Cluster outcomes plot saved to {outfile}")
PYOUT
else
  echo "python3 not found. Skipping outcomes plot."
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
if [ -f "$CLUSTER_VIZ_FILE" ]; then
  mv "$CLUSTER_VIZ_FILE" "$OUT_DIR/cluster_viz.png"
fi
mv "$LEFT_OUTCOME_FILE" "$OUT_DIR/left_outcome.tsv"
mv "$CLUSTER_OUTCOMES_FILE" "$OUT_DIR/cluster_outcomes.tsv"
if [ -f "$CLUSTER_OUTCOMES_PNG" ]; then
  mv "$CLUSTER_OUTCOMES_PNG" "$OUT_DIR/cluster_outcomes_plot.png"
fi

echo "All outputs moved to $OUT_DIR/ for project submission."
echo "Pipeline complete. Outputs generated."
echo "To use a different dataset, run: $0 <path_to_your_csv_file>"
