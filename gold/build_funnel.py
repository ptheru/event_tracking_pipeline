from pyspark.sql import SparkSession, functions as F
from configs.spark_session import get_spark
from configs.paths import FACT_EVENTS_PATH, GOLD_DAU_PATH

spark = get_spark("build_funnel")

df = spark.read.parquet(FACT_EVENTS_PATH)

funnel = (
    df.filter(F.col("event_type").isin("view", "start", "complete"))
      .groupBy("event_date", "event_type")
      .agg(F.countDistinct("session_id").alias("sessions"))
)

(funnel.coalesce(4)
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(GOLD_DAU_PATH))
