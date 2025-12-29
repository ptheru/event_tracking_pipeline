from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from configs.spark_session import get_spark
from configs.paths import (
    SILVER_EVENTS_PATH,
    DIM_SESSION_PATH
)

spark = get_spark("build_dim_session")

df = spark.read.parquet(SILVER_EVENTS_PATH)

dim_session = (
    df.select("session_id", "user_id", "device_type", "country")
      .dropDuplicates(["session_id"])
      .withColumn("session_key", F.monotonically_increasing_id())
)

dim_session.write.mode("overwrite").parquet(DIM_SESSION_PATH)
