from pyspark.sql import SparkSession, functions as F
from configs.spark_session import get_spark
from configs.spark_session import get_spark
from configs.paths import (
    SILVER_EVENTS_PATH,
    DIM_USER_PATH,
    DIM_VIDEO_PATH,
    DIM_SESSION_PATH,
    FACT_EVENTS_PATH
)

spark = get_spark("build_fact_events")


events = spark.read.parquet(SILVER_EVENTS_PATH)
dim_user = spark.read.parquet(DIM_USER_PATH)
dim_video = spark.read.parquet(DIM_VIDEO_PATH)
dim_session = spark.read.parquet(DIM_SESSION_PATH)

# Broadcast small dimension tables
dim_user_b = F.broadcast(dim_user)
dim_video_b = F.broadcast(dim_video)
dim_session_b = F.broadcast(dim_session)

events_rep = events.repartition(200, "event_date")

fact = (
    events_rep.alias("e")
        .join(dim_user_b.alias("u"), "user_id", "left")
        .join(dim_video_b.alias("v"), "video_id", "left")
        .join(dim_session_b.alias("s"), "session_id", "left")
        .select(
            "event_id", "event_ts", "event_date", "event_type",
            "user_id", "user_key",
            "video_id", "video_key",
            "session_id", "session_key",
            "country", "device_type", "app_version", "ingest_ts"
        )
)

# We want ~256MB files.
# For 50GB:
#   50GB = 50,000MB => 50,000 / 256 ≈ 195 files.
# We round to 200 and partition by event_date to keep things balanced.

fact_rep = fact.repartition(200, "event_date") 


(fact_rep.write
   .mode("overwrite")
   .partitionBy("event_date")
   .parquet(FACT_EVENTS_PATH))
