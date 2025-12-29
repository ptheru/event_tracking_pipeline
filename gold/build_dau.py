from pyspark.sql import SparkSession, functions as F
from configs.spark_session import get_spark
from configs.paths import FACT_EVENTS_PATH, GOLD_DAU_PATH

spark = get_spark("build_dau")


df = spark.read.parquet(FACT_EVENTS_PATH)

dau = (
    df.select("event_date", "user_id")
      .dropDuplicates()
      .groupBy("event_date")
      .agg(F.count("*").alias("dau"))
)


# Gold table is typically MUCH smaller – we avoid repartition() (no shuffle),
# just reduce files via coalesce().

(dau.coalesce(4)
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(GOLD_DAU_PATH))
