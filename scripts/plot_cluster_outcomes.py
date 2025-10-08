import sys
import matplotlib.pyplot as plt
import pandas as pd

if len(sys.argv) < 3:
    print("Usage: plot_cluster_outcomes.py <cluster_outcomes.tsv> <output_png>")
    sys.exit(1)

infile = sys.argv[1]
outfile = sys.argv[2]

df = pd.read_csv(infile, sep='\t', header=None, names=['Entity', 'Count', 'Mean', 'Median'])
plt.figure(figsize=(8,5))
plt.bar(df['Entity'], df['Mean'], color='skyblue')
plt.xlabel('Entity')
plt.ylabel('Mean Outcome')
plt.title('Mean Outcome by Cluster Entity')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(outfile)
print(f"Cluster outcomes plot saved to {outfile}")
