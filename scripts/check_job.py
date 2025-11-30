import os, time
from pyspark.sql import SparkSession

bucket = os.environ.get("BUCKET")
assert bucket, "BUCKET env var must be set"
out_path = f"gs://{bucket}/output/"

spark = SparkSession.builder.appName("hw8_parquet").getOrCreate()

df = spark.range(0, 1_000_000).withColumnRenamed("id", "num")
df.createOrReplaceTempView("nums")

agg = spark.sql("""
  SELECT (num % 10) AS bucket, COUNT(*) AS c
  FROM nums
  GROUP BY (num % 10)
  ORDER BY bucket
""")

agg.write.mode("overwrite").parquet(out_path)

time.sleep(20)  # keep UI alive for screenshot
spark.stop()
