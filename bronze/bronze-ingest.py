from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os
from configs.spark_session import get_spark
from configs.paths import RAW_EVENTS_PATH, BRONZE_BASE_PATH

spark = get_spark("bronze_ingest")


schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("video_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("device_type", StringType()),
    StructField("country", StringType()),
    StructField("app_version", StringType()),
    StructField("ingest_ts", TimestampType()),
])


df = (
    spark.read
         .schema(schema)
         .json(RAW_EVENTS_PATH)
         .withColumn("event_date", F.to_date("event_ts"))
)

(df.write
   .mode("overwrite")
   .partitionBy("event_date")
   .parquet(BRONZE_BASE_PATH))
