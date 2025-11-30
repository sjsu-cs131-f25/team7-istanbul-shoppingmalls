import time
from pyspark.sql import SparkSession

bucket = "cs131-f25-hw8-yix-hw8"      # hardcode your bucket name
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
time.sleep(20)
spark.stop()
