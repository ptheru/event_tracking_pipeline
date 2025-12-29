from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from configs.spark_session import get_spark
from configs.paths import (
    SILVER_EVENTS_PATH,
    BRONZE_BASE_PATH
)

spark = get_spark("transform_events")

df = spark.read.parquet(BRONZE_BASE_PATH)

df_clean = (
    df.filter(F.col("event_ts").isNotNull())
      .withColumn("event_date", F.to_date("event_ts"))
      .withColumn("country", F.upper("country"))
      .withColumn("device_type", F.lower("device_type"))
)

(df_clean.write
   .mode("overwrite")
   .partitionBy("event_date")
   .parquet(SILVER_EVENTS_PATH))
