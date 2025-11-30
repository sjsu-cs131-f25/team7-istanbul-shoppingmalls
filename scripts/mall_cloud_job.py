#!/usr/bin/env python3
"""
PySpark job for Team 7 — Istanbul Shopping Malls (2021–2023)


Inputs (CSV with header):
  invoice_no,customer_id,gender,age,category,quantity,price,payment_method,invoice_date,shopping_mall

Outputs (Parquet by default):
  silver.transactions_clean                                # cleaned, filtered transactions
  gold.customer_summary                                     # per-customer KPIs
  gold.monthly_summary                                      # year-month KPIs
  gold.category_avg_spend                                   # avg spend per tx by category (+ top10)

Run examples:
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    mall_cloud_job.py \
    --input s3://team7-malls/bronze/customer_shopping_data.csv \
    --out-base s3://team7-malls/ \
    --format parquet

  # Databricks notebook: just paste in a cell; set IN/OUT; run.

Notes:
- Partitioning by year_month and shopping_mall.
- Quality filters align with your bash scripts (qty/price bounds; drop TEST/DUMMY; non-empty ids).
- Includes derived fields and buckets.
"""

import argparse
from pyspark.sql import SparkSession, functions as F, types as T

# ------------ Args ------------
parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True, help='Cloud path to raw CSV')
parser.add_argument('--out-base', required=True, help='Cloud base path, e.g., s3://bucket/')
parser.add_argument('--format', default='parquet', choices=['parquet','delta','csv'])
parser.add_argument('--mode', default='overwrite', choices=['overwrite','append'])
args = parser.parse_args()

OUT_SILVER = args.out_base.rstrip('/') + '/silver/transactions_clean'
OUT_GOLD_CUST = args.out_base.rstrip('/') + '/gold/customer_summary'
OUT_GOLD_MONTH = args.out_base.rstrip('/') + '/gold/monthly_summary'
OUT_GOLD_CAT = args.out_base.rstrip('/') + '/gold/category_avg_spend'
OUT_GOLD_TOP10 = args.out_base.rstrip('/') + '/gold/category_top10'

# ------------ Spark ------------
spark = (
    SparkSession.builder.appName('team7-malls-final')
    .config('spark.sql.shuffle.partitions', '200')
    .config('spark.sql.adaptive.enabled', 'true')
    .config('spark.sql.adaptive.skewJoin.enabled', 'true')
    .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    .getOrCreate()
)

# ------------ Schema ------------
schema = T.StructType([
    T.StructField('invoice_no', T.StringType()),
    T.StructField('customer_id', T.StringType()),
    T.StructField('gender', T.StringType()),
    T.StructField('age', T.IntegerType()),
    T.StructField('category', T.StringType()),
    T.StructField('quantity', T.DoubleType()),
    T.StructField('price', T.DoubleType()),
    T.StructField('payment_method', T.StringType()),
    T.StructField('invoice_date', T.StringType()),  # will parse into date
    T.StructField('shopping_mall', T.StringType()),
])

raw = (
    spark.read.option('header','true').schema(schema).csv(args.input)
)

# ------------ Clean & Normalize ------------
trim_all = [F.trim(F.col(c)).alias(c) if raw.schema[c].dataType == T.StringType() else F.col(c) for c in raw.columns]
raw_trim = raw.select(*trim_all)

# Handle price/quantity coercion
raw_cast = (
    raw_trim
    .withColumn('quantity', F.col('quantity').cast('double'))
    .withColumn('price', F.col('price').cast('double'))
)

# Invoice date parsing: try multiple formats from prior scripts (e.g., d/m/Y or m/d/Y)
parse_formats = ['d/M/yyyy', 'M/d/yyyy', 'yyyy-M-d', 'yyyy/MM/dd']
exprs = [F.to_date('invoice_date', fmt) for fmt in parse_formats]
invoice_dt = exprs[0]
for e in exprs[1:]:
    invoice_dt = F.coalesce(invoice_dt, e)

clean = (
    raw_cast
    .withColumn('invoice_dt', invoice_dt)
    .withColumn('invoice_year', F.year(F.col('invoice_dt')))
    .withColumn('invoice_month', F.month(F.col('invoice_dt')))
    .withColumn('year_month', F.date_format(F.col('invoice_dt'), 'yyyy-MM'))
    .withColumn('category_lower', F.lower(F.col('category')))
)

# ------------ Quality Filters (match bash rules) ------------
filtered = (
    clean
    .filter(F.col('customer_id').isNotNull() & (F.col('customer_id') != ''))
    .filter(F.col('invoice_no').isNotNull() & (F.col('invoice_no') != ''))
    .filter(~F.col('invoice_no').rlike('(?i)TEST'))
    .filter(~F.col('category_lower').rlike('(?i)test|dummy'))
    .filter((F.col('quantity') > 0) & (F.col('quantity') <= 1000))
    .filter((F.col('price') > 0) & (F.col('price') <= 100000))
)

# ------------ Silver output ------------
writer = filtered.repartition('year_month', 'shopping_mall')
if args.format == 'delta':
    (writer.write.format('delta').mode(args.mode)
     .partitionBy('year_month','shopping_mall').save(OUT_SILVER))
elif args.format == 'parquet':
    (writer.write.format('parquet').option('compression','zstd').mode(args.mode)
     .partitionBy('year_month','shopping_mall').save(OUT_SILVER))
else:
    (writer.write.format('csv').option('header','true').mode(args.mode)
     .save(OUT_SILVER))

# ------------ GOLD: Per-customer Summary ------------
# total_spend = quantity * price; avg_price_per_item = total_spend / total_items
cust = (
    filtered
    .withColumn('spend', F.col('quantity') * F.col('price'))
    .groupBy('customer_id')
    .agg(
        F.count('*').alias('transactions'),
        F.sum('quantity').alias('total_items'),
        F.sum('spend').alias('total_spend'),
        F.avg('price').alias('price_mean'),
        F.expr('percentile_approx(price, 0.5)').alias('price_median'),
        F.avg((F.col('price') > 100).cast('double')).alias('high_price_tx_ratio')
    )
    .withColumn('avg_price_per_item',
                F.when(F.col('total_items') > 0, F.col('total_spend')/F.col('total_items')).otherwise(F.lit(0.0)))
    .withColumn('bucket', F.when(F.col('total_items') == 0, F.lit('ZERO'))
                          .when(F.col('avg_price_per_item') < 10, F.lit('LO'))
                          .when(F.col('avg_price_per_item') <= 100, F.lit('MID'))
                          .otherwise(F.lit('HI')))
)

(cust.write.format(args.format if args.format!='delta' else 'delta')
     .mode(args.mode)
     .save(OUT_GOLD_CUST))

# ------------ GOLD: Monthly Summary (align with awk Step 5) ------------
month = (
    filtered
    .groupBy('year_month')
    .agg(
        F.count('*').alias('tx_count'),
        F.avg('price').alias('avg_price')
    )
    .orderBy('year_month')
)

(month.write.format(args.format if args.format!='delta' else 'delta')
      .mode(args.mode)
      .save(OUT_GOLD_MONTH))

# ------------ GOLD: Category Avg Spend per Transaction (Step 6 equivalent) ------------
cat = (
    filtered
    .withColumn('spend', F.col('quantity')*F.col('price'))
    .groupBy('category')
    .agg(
        F.count('*').alias('tx'),
        F.sum('spend').alias('total_spend'),
        (F.sum('spend')/F.count('*')).alias('avg_spend_per_tx')
    )
    .orderBy(F.col('avg_spend_per_tx').desc())
)

(cat.write.format(args.format if args.format!='delta' else 'delta')
    .mode(args.mode)
    .save(OUT_GOLD_CAT))

# Top 10 as a small CSV for easy plotting outside Spark
(cat.limit(10)
   .coalesce(1)
   .write.mode('overwrite').option('header','true').csv(OUT_GOLD_TOP10))

print('\n=== SUCCESS ===')
print(f'Silver: {OUT_SILVER}')
print(f'Gold (customers): {OUT_GOLD_CUST}')
print(f'Gold (monthly):   {OUT_GOLD_MONTH}')
print(f'Gold (category):  {OUT_GOLD_CAT}')


