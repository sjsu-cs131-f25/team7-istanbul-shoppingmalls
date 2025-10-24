#!/usr/bin/env bash
# Project Assignment 4: Steps 1 - 2
set -e
# Resolve repo root relative to this script so INPUT works regardless of cwd
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_INPUT="$REPO_ROOT/data/Shopping Mall Data in Istanbul/customer_shopping_data.csv"
INPUT="${1:-$DEFAULT_INPUT}"
OUTDIR="${2:-$REPO_ROOT/out_project4}"
mkdir -p "$OUTDIR"
echo "Project 4 starting...."

# Step 1 - Clean & normalize (SED)
echo cleaning white spaces
sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//; s/[[:space:]]*,[[:space:]]*/,/g; s/([0-9]),([0-9]{3})/\1\2/g; s/,{2,}/,NA,/g; s/,$/,NA/; s/^,/NA,/' \
        "$INPUT" > "$OUTDIR/step1_cleaned.csv"
# before/after samples
head -n 10 "$INPUT" > "$OUTDIR/step1_before.csv"
head -n 10 "$OUTDIR/step1_cleaned.csv" > "$OUTDIR/step1_after.csv"

echo Step 1 complete

# Step 2 - Skinny tables & frequency tables
echo sorting and creating frequency tables
# creating gender frequency table
cut -d, -f3 "$OUTDIR/step1_cleaned.csv" | tail -n +2 | sort | uniq -c | sort -k1,1nr | \
        awk '
                BEGIN{print "gender\tcount"} {print $2 "\t" $1}' > "$OUTDIR/freq_gender.tsv"

# creating shopping mall frequency table
awk -F, 'NR>1 {
    mall = $10
    gsub(/^"|"$/, "", mall)            # remove quotes if any
    gsub(/^[ \t]+|[ \t]+$/, "", mall)  # trim spaces
    if (mall != "") counts[mall]++
}
END {
    print "shopping_mall\tcount"
    for (m in counts) printf "%s\t%d\n", m, counts[m]
}' "$OUTDIR/step1_cleaned.csv" | sort -k2,2nr > "$OUTDIR/freq_shopping_mall.tsv"



echo creating Top-N list
# creating Top-N list
# $2 = customer_id $6 = quantity
awk -F, 'NR>1 {sum[$2]+=$6}
END {print "customer_id\ttotal_quantity"; for (c in sum) print c "\t" sum[c]}' \
"$OUTDIR/step1_cleaned.csv" | sort -k2,2nr | head -n10 > "$OUTDIR/top10_customers_by_quantity.tsv"

echo creating skinny table
# skinny table consists: 1 = invoice no, 2 = customer_id, 5 = category, 6 = quantity, 7 = price
head -n1 "$OUTDIR/step1_cleaned.csv" | cut -d, -f1,2,5,6,7 > "$OUTDIR/skinny_table.tsv"
tail -n +2 "$OUTDIR/step1_cleaned.csv" | cut -d, -f1,2,5,6,7 >> "$OUTDIR/skinny_table.tsv"

echo Step 2 complete

##############################
# Step 3 - Quality filters
##############################
echo applying quality filters Step 3
# produce a tab-separated filtered file, keep header; predicates:
# - non-empty customer_id and invoice
# - qty > 0 and qty <= 1000
# - price > 0 and price <= 100000
# - drop rows with TEST/DUMMY in invoice or category
awk -F, -v OFS='\t' 'NR==1 {
    # print header as TSV (preserve up to 10 columns)
    for(i=1;i<=NF;i++) gsub(/^[ \t]+|[ \t]+$/,"",$i)
    print $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
    next
}
{
    # trim quotes and spaces from each field
    for(i=1;i<=NF;i++){ gsub(/^"|"$/,"",$i); gsub(/^[ \t]+|[ \t]+$/,"",$i) }
    inv=$1; cust=$2; cat=$5; qty=($6+0); price=($7+0)
    # predicates
    if (cust=="" || cust=="NA") next
    if (inv=="" || inv ~ /TEST/i) next
    if (qty <= 0 || qty > 1000) next
    if (price <= 0 || price > 100000) next
    if (tolower(cat) ~ /test|dummy/) next
    # emit as TSV
    print $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
}' "$OUTDIR/step1_cleaned.csv" > "$OUTDIR/filtered_transactions.tsv"

echo Step 3 complete

##############################
# Step 4 - Ratios, buckets, per-entity summaries
##############################
echo computing per-customer summaries and buckets Step 4
# For each customer compute:
# - transactions (number of rows)
# - total_items (sum qty)
# - total_spend (sum qty * price)
# - avg_price_per_item = total_spend / total_items (guard divide-by-zero)
# - high_price_tx_ratio = transactions with price>100 / transactions
# Bucket by avg_price_per_item: ZERO (items==0), LO (<10), MID (10-100), HI (>100)

awk -F'\t' -v OUT="$OUTDIR" 'NR>1 {
    cust=$2; qty=($6+0); price=($7+0)
    tx[cust]++
    items[cust]+=qty
    spend[cust]+=qty*price
    high_tx[cust]+= (price>100 ? 1 : 0)
}
END{
    # header for customer summary
    printf "customer_id\ttransactions\ttotal_items\ttotal_spend\tavg_price_per_item\thigh_price_tx_ratio\tbucket\n" > (OUT "/customer_summary.tsv")
    for (c in items) {
        avg = (items[c] > 0) ? (spend[c]/items[c]) : 0
        ratio = (tx[c] > 0) ? (high_tx[c]/tx[c]) : 0
        if (items[c]==0) bucket="ZERO"
        else if (avg < 10) bucket="LO"
        else if (avg <= 100) bucket="MID"
        else bucket="HI"
        printf "%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%s\n", c, tx[c], items[c], spend[c], avg, ratio, bucket >> (OUT "/customer_summary.tsv")
        bucket_count[bucket]++
    }
    # print bucket counts
    printf "bucket\tcount\n" > (OUT "/bucket_counts.tsv")
    for (b in bucket_count) printf "%s\t%d\n", b, bucket_count[b] >> (OUT "/bucket_counts.tsv")
}' "$OUTDIR/filtered_transactions.tsv"

echo Step 4 complete

# Step 5
# Temporal structure
echo "Starting Step 5"

awk -F, 'NR>1 {
  split($9, d, "/")
  month = (length(d[2]) == 1 ? "0"d[2] : d[2])
  year = d[3]
  ym = year "-" month

  count[ym]++
  total[ym] += $7
}
END {
  for (m in count) {
    printf "%s\t%d\t%.2f\n", m, count[m], total[m]/count[m]
  }
}' "$INPUT" | sort | awk 'BEGIN{print "month\tcount\tavg_price"} {print}' > "$OUTDIR/monthly_summary.tsv"


echo "created monthly_summary.tsv"
echo "Step 5 complete"

##############################
# Step 6 - Signal discovery (simple)
##############################
echo "Step 6: simple signals (numeric stats + top categories)"

# One pass: numeric stats and category avg spend per tx
awk -F, -v out="$OUTDIR" '
NR>1 {
  # quantity (col 6), price (col 7) — strip quotes then coerce to number
  qf=$6; pf=$7
  gsub(/^"|"$/,"",qf); gsub(/^"|"$/,"",pf)
  q=qf+0; p=pf+0

  if(qf!=""){ nq++; sq+=q; sq2+=q*q; if(minq==""||q<minq) minq=q; if(maxq==""||q>maxq) maxq=q }
  if(pf!=""){ np++; sp+=p; sp2+=p*p; if(minp==""||p<minp) minp=p; if(maxp==""||p>maxp) maxp=p }

  # category (col 5)
  cat=$5; gsub(/^"|"$/,"",cat); gsub(/^[ \t]+|[ \t]+$/,"",cat)
  if(cat!=""){ cat_tx[cat]++; cat_spend[cat]+=q*p }
}
END {
  # numeric distributions
  fn = out "/numeric_distributions.tsv"
  printf "feature\tcount\tmean\tstddev\tmin\tmax\n" > fn
  if(nq>0){ m=sq/nq; sd=sqrt((sq2/nq)-(m*m)); printf "quantity\t%d\t%.2f\t%.2f\t%.2f\t%.2f\n", nq,m,sd,minq,maxq >> fn }
  if(np>0){ m=sp/np; sd=sqrt((sp2/np)-(m*m)); printf "price\t%d\t%.2f\t%.2f\t%.2f\t%.2f\n", np,m,sd,minp,maxp >> fn }

  # dump category avg spend per tx to a temp file for sorting
  tmp = out "/.category_avg.tmp"
  for(c in cat_tx){
    avg = (cat_tx[c]>0 ? cat_spend[c]/cat_tx[c] : 0)
    printf "%s\t%.2f\n", c, avg >> tmp
  }
}' "$OUTDIR/step1_cleaned.csv"

# Top 10 categories by avg spend per tx
sort -k2,2nr "$OUTDIR/.category_avg.tmp" | head -n 10 > "$OUTDIR/category_top_avg.tsv"

# Minimal ranked signals table
{
  echo -e "signal\tkind\tvalue"
  awk -F'\t' 'NR==2{print "quantity_mean\tsimple\t"$3} NR==3{print "price_mean\tsimple\t"$3}' "$OUTDIR/numeric_distributions.tsv"
  awk -F'\t' '{printf "%s\tcategory_avg_spend\t%s\n",$1,$2}' "$OUTDIR/category_top_avg.tsv"
} > "$OUTDIR/signals_ranked.tsv"

# cleanup
rm -f "$OUTDIR/.category_avg.tmp"

echo "Step 6 complete"
echo "All steps complete. outputs in: $OUTDIR"