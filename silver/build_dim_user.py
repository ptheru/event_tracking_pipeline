from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from configs.spark_session import get_spark
from configs.paths import (
    SILVER_EVENTS_PATH,
    DIM_USER_PATH
)

spark = get_spark("build_dim_user")

df = spark.read.parquet(SILVER_EVENTS_PATH)

dim_user = (
    df.select("user_id", "country", "device_type", "app_version")
      .dropDuplicates(["user_id"])
      .withColumn("user_key", F.monotonically_increasing_id())
)

dim_user.write.mode("overwrite").parquet(DIM_USER_PATH)
