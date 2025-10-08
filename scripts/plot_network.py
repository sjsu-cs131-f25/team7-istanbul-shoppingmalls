

import sys
import networkx as nx
import matplotlib.pyplot as plt
import random

if len(sys.argv) < 3:
    print("Usage: plot_network.py <edges_file> <output_png>")
    sys.exit(1)

edges_file = sys.argv[1]
output_png = sys.argv[2]

# Read all edges
with open(edges_file) as f:
    edges = [line.strip().split('\t') for line in f if '\t' in line]

# Sample 500 edges for visualization
sample_size = min(500, len(edges))
edges_sample = random.sample(edges, sample_size)

G = nx.Graph()
G.add_edges_from(edges_sample)

# Color nodes by entity type (left entity)
node_colors = []
for node in G.nodes():
    # If node appears as left entity in any edge, color by type
    left_entities = set(e[0] for e in edges_sample)
    if node in left_entities:
        if node == "Female":
            node_colors.append('skyblue')
        elif node == "Male":
            node_colors.append('lightcoral')
        else:
            node_colors.append('lightgreen')
    else:
        node_colors.append('gray')

# Show labels for up to 50 nodes
labels = {}
for i, node in enumerate(G.nodes()):
    if i < 50:
        labels[node] = node

plt.figure(figsize=(10,10))
pos = nx.spring_layout(G, seed=42)
nx.draw(G, pos, node_size=100, node_color=node_colors, with_labels=False, edge_color='lightgray')
nx.draw_networkx_labels(G, pos, labels, font_size=8)
plt.title(f"Network Visualization (Sample of {sample_size} edges, up to 50 labels)")
plt.savefig(output_png)
print(f"Network visualization saved to {output_png}")
