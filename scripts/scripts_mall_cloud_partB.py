"""
Part B — Additional Analysis / Creativity
Team 7 — Istanbul Shopping Malls (2021–2023)

This script assumes Part A (mall_cloud_job.py) has already run and produced:
  silver/transactions_clean   # cleaned, filtered, partitioned by year_month & shopping_mall

It reads the silver table and generates additional analyses for Section B of the report:

Outputs (under --out-base):
  gold_b.invoice_baskets          # per-invoice basket KPIs
  gold_b.mall_month_extended      # mall × month revenue & basket stats
  gold_b.age_band_category        # age-band × category revenue & quantity
  gold_b.payment_behavior         # mall × age-band × payment method revenue
  gold_b.mall_counts              # mall-level row counts (skew check)
  gold_b.mall_agg_skewed          # skewed mall agg (no repartition)
  gold_b.mall_agg_balanced        # balanced mall agg (with repartition)
  gold_b.mall_month_baseline      # baseline mall×month agg (no filter)
  gold_b.mall_month_optimized     # optimized agg (with filter → partition pruning)
  gold_b.join_regular             # regular join with mall dimension
  gold_b.join_broadcast           # broadcast join with mall dimension
  gold_b.ml_predictions (optional)# simple high-spend basket model (if --run-ml true)

Example:
  spark-submit \
    mall_cloud_partB.py \
    --silver s3://team7-malls/silver/transactions_clean \
    --out-base s3://team7-malls/ \
    --format parquet

On Dataproc with GCS, just swap s3:// for gs://.
"""

import argparse
from pyspark.sql import SparkSession, functions as F

# ------------ Args ------------
parser = argparse.ArgumentParser()
parser.add_argument('--silver', required=True,
                    help='Cloud path to silver/transactions_clean (from Part A)')
parser.add_argument('--out-base', required=True,
                    help='Cloud base path, e.g., s3://bucket/ or gs://bucket/')
parser.add_argument('--format', default='parquet',
                    choices=['parquet', 'delta', 'csv'],
                    help='Output format for Part B tables')
parser.add_argument('--mode', default='overwrite',
                    choices=['overwrite', 'append'],
                    help='Save mode')
parser.add_argument('--run-ml', action='store_true',
                    help='Run optional simple ML model on basket spend')
args = parser.parse_args()

base = args.out_base.rstrip('/')

OUT_B_BASKETS        = base + '/gold_b/invoice_baskets'
OUT_B_MALL_MONTH_EXT = base + '/gold_b/mall_month_extended'
OUT_B_AGE_CAT        = base + '/gold_b/age_band_category'
OUT_B_PAY_BEHAV      = base + '/gold_b/payment_behavior'
OUT_B_MALL_COUNTS    = base + '/gold_b/mall_counts'
OUT_B_MALL_SKEWED    = base + '/gold_b/mall_agg_skewed'
OUT_B_MALL_BALANCED  = base + '/gold_b/mall_agg_balanced'
OUT_B_MALL_BASE      = base + '/gold_b/mall_month_baseline'
OUT_B_MALL_OPT       = base + '/gold_b/mall_month_optimized'
OUT_B_JOIN_REG       = base + '/gold_b/join_regular'
OUT_B_JOIN_BCAST     = base + '/gold_b/join_broadcast'
OUT_B_ML_PRED        = base + '/gold_b/ml_predictions'

# ------------ Spark ------------
spark = (
    SparkSession.builder
    .appName('team7-malls-partB')
    .config('spark.sql.shuffle.partitions', '200')
    .config('spark.sql.adaptive.enabled', 'true')
    .config('spark.sql.adaptive.skewJoin.enabled', 'true')
    .getOrCreate()
)

# ------------ Read silver table ------------
print(f"Reading silver table from: {args.silver}")

if args.format == 'delta':
    silver = spark.read.format('delta').load(args.silver)
elif args.format == 'parquet':
    silver = spark.read.parquet(args.silver)
else:  # csv (less likely here, but supported)
    silver = spark.read.option('header', 'true').csv(args.silver)

silver.printSchema()
silver.show(5, truncate=False)


# Add missing derived columns that Part A never wrote into silver
silver = (
    silver
    .withColumn('spend', F.col('quantity') * F.col('price'))
    .withColumn(
        'age_band',
        F.when(F.col('age') < 20, '0-19')
         .when(F.col('age') < 30, '20-29')
         .when(F.col('age') < 40, '30-39')
         .when(F.col('age') < 50, '40-49')
         .when(F.col('age') < 60, '50-59')
         .otherwise('60+')
    )
)


# Expecting columns from Part A like:
# invoice_no, customer_id, gender, age, category, quantity, price,
# payment_method, invoice_date, shopping_mall,
# invoice_dt, invoice_year, invoice_month, year_month,
# category_lower, spend, age_band, ...


# =====================================================================
# ====================  B1. BASKET-LEVEL ANALYSIS  ====================
# =====================================================================
print("\n=== B1: Basket-level KPIs (per invoice) ===")

baskets = (
    silver
    .groupBy('invoice_no', 'customer_id', 'shopping_mall', 'year_month')
    .agg(
        F.sum('spend').alias('basket_spend'),
        F.sum('quantity').alias('basket_items'),
        F.countDistinct('category').alias('basket_category_count')
    )
)

baskets.show(10, truncate=False)

(baskets
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_BASKETS))


# =====================================================================
# ==========  B2. MALL × MONTH EXTENDED (BASKET SUMMARY)  =============
# =====================================================================
print("\n=== B2: Mall × Month extended summary ===")

mall_month_ext = (
    baskets
    .groupBy('shopping_mall', 'year_month')
    .agg(
        F.sum('basket_spend').alias('total_revenue'),
        F.count('*').alias('num_invoices'),
        F.avg('basket_spend').alias('avg_basket_value'),
        F.avg('basket_items').alias('avg_basket_items'),
    )
    .orderBy('shopping_mall', 'year_month')
)

mall_month_ext.show(20, truncate=False)

(mall_month_ext
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_MALL_MONTH_EXT))


# =====================================================================
# ===================  B3. AGE-BAND × CATEGORY  =======================
# =====================================================================
print("\n=== B3: Age-band × Category summary ===")

# age_band column was already created in Part A
age_cat = (
    silver
    .groupBy('age_band', 'category')
    .agg(
        F.sum('spend').alias('total_revenue'),
        F.sum('quantity').alias('items_sold')
    )
    .orderBy('age_band', F.desc('total_revenue'))
)

age_cat.show(20, truncate=False)

(age_cat
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_AGE_CAT))


# =====================================================================
# =======  B4. PAYMENT BEHAVIOR: MALL × AGE-BAND × METHOD  ============
# =====================================================================
print("\n=== B4: Payment behavior (Mall × Age-band × Payment method) ===")

pay_beh = (
    silver
    .groupBy('shopping_mall', 'age_band', 'payment_method')
    .agg(
        F.sum('spend').alias('total_revenue')
    )
)

pay_beh.show(20, truncate=False)

(pay_beh
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_PAY_BEHAV))


# =====================================================================
# ===========  B5. SKEW CHECK & REPARTITIONED AGG  ====================
# =====================================================================
print("\n=== B5: Skew check & repartitioned aggregation ===")

mall_counts = (
    silver
    .groupBy('shopping_mall')
    .count()
    .orderBy(F.desc('count'))
)

mall_counts.show(truncate=False)

(mall_counts
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_MALL_COUNTS))

# Skewed aggregation (no explicit repartition)
mall_agg_skewed = (
    silver
    .groupBy('shopping_mall')
    .agg(F.sum('spend').alias('total_revenue'))
)

mall_agg_skewed.explain(mode='formatted')

(mall_agg_skewed
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_MALL_SKEWED))

# Balanced aggregation using repartition on shopping_mall
mall_agg_balanced = (
    silver
    .repartition(8, 'shopping_mall')
    .groupBy('shopping_mall')
    .agg(F.sum('spend').alias('total_revenue'))
)

mall_agg_balanced.explain(mode='formatted')

(mall_agg_balanced
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_MALL_BALANCED))


# =====================================================================
# ======  B6. BASELINE VS OPTIMIZED (PARTITION PRUNING DEMO)  =========
# =====================================================================
print("\n=== B6: Baseline vs optimized mall×month aggregation ===")

# Baseline: no filter, group on the full silver DataFrame
mall_month_baseline = (
    silver
    .groupBy('shopping_mall', 'year_month')
    .agg(F.sum('spend').alias('total_revenue'))
)

mall_month_baseline.explain(mode='formatted')
_ = mall_month_baseline.count()  # trigger job so you can inspect Spark UI

(mall_month_baseline
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_MALL_BASE))

# Optimized: filter on a specific year to leverage partition pruning (since silver is partitioned)
# Adjust year as needed (2021, 2022, 2023 exist in the dataset)
print("\nRunning optimized aggregation with a year filter (e.g., 2021) to show partition pruning.")
mall_month_optimized = (
    silver
    .filter(F.col('invoice_year') == 2021)
    .groupBy('shopping_mall', 'year_month')
    .agg(F.sum('spend').alias('total_revenue'))
)

mall_month_optimized.explain(mode='formatted')
_ = mall_month_optimized.count()

(mall_month_optimized
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_MALL_OPT))


# =====================================================================
# ================  B7. JOIN OPTIMIZATION DEMO  =======================
# =====================================================================
print("\n=== B7: Join optimization (regular vs broadcast) ===")

# Build a small mall dimension from distinct malls
mall_names = [row['shopping_mall'] for row in silver.select('shopping_mall').distinct().collect()]
mall_dim_data = [(m, 'General') for m in mall_names]
mall_dim = spark.createDataFrame(mall_dim_data, ['shopping_mall', 'mall_segment'])

# Regular join
join_reg = silver.join(mall_dim, 'shopping_mall', 'left')
join_reg.explain(mode='formatted')

(join_reg
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_JOIN_REG))

# Broadcast join
from pyspark.sql.functions import broadcast

join_bcast = silver.join(broadcast(mall_dim), 'shopping_mall', 'left')
join_bcast.explain(mode='formatted')

(join_bcast
 .write
 .format(args.format if args.format != 'delta' else 'delta')
 .mode(args.mode)
 .save(OUT_B_JOIN_BCAST))


# =====================================================================
# ====================  B8. OPTIONAL ML MODEL  ========================
# =====================================================================
if args.run_ml:
    print("\n=== B8: Simple ML model — predict high-spend basket ===")
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml import Pipeline
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    # Use the baskets table built earlier
    baskets_ml = (
        baskets
        .join(mall_dim, 'shopping_mall', 'left')
        .withColumn('high_spend', (F.col('basket_spend') > 500).cast('int'))
    )

    indexer = StringIndexer(
        inputCol='mall_segment',
        outputCol='mall_segment_idx',
        handleInvalid='keep'
    )

    encoder = OneHotEncoder(
        inputCols=['mall_segment_idx'],
        outputCols=['mall_segment_oh']
    )

    assembler = VectorAssembler(
        inputCols=['basket_items', 'basket_category_count', 'mall_segment_oh'],
        outputCol='features'
    )

    lr = LogisticRegression(
        featuresCol='features',
        labelCol='high_spend',
        maxIter=50
    )

    pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])

    train_df, test_df = baskets_ml.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train_df)
    pred = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(
        labelCol='high_spend',
        metricName='areaUnderROC'
    )
    auc = evaluator.evaluate(pred)
    print(f"[ML] High-spend basket AUC: {auc}")

    (pred
     .write
     .format(args.format if args.format != 'delta' else 'delta')
     .mode(args.mode)
     .save(OUT_B_ML_PRED))

# ------------ Final logs ------------
print("\n=== PART B SUCCESS ===")
print(f'Invoice baskets:         {OUT_B_BASKETS}')
print(f'Mall×Month extended:     {OUT_B_MALL_MONTH_EXT}')
print(f'Age-band×Category:       {OUT_B_AGE_CAT}')
print(f'Payment behavior:        {OUT_B_PAY_BEHAV}')
print(f'Mall counts (skew):      {OUT_B_MALL_COUNTS}')
print(f'Mall agg (skewed):       {OUT_B_MALL_SKEWED}')
print(f'Mall agg (balanced):     {OUT_B_MALL_BALANCED}')
print(f'Mall×Month baseline:     {OUT_B_MALL_BASE}')
print(f'Mall×Month optimized:    {OUT_B_MALL_OPT}')
print(f'Join (regular):          {OUT_B_JOIN_REG}')
print(f'Join (broadcast):        {OUT_B_JOIN_BCAST}')
if args.run_ml:
    print(f'ML predictions:          {OUT_B_ML_PRED}')
