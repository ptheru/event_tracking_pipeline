from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from configs.spark_session import get_spark
from configs.paths import (
    SILVER_EVENTS_PATH,
    DIM_VIDEO_PATH
)

spark = get_spark("build_dim_video")

df = spark.read.parquet(SILVER_EVENTS_PATH)

dim_video = (
    df.select("video_id")
      .dropDuplicates(["video_id"])
      .withColumn("video_key", F.monotonically_increasing_id())
)

dim_video.write.mode("overwrite").parquet(DIM_VIDEO_PATH)
