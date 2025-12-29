from pyspark.sql import SparkSession

def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
            .appName(app_name)

            # ---- Cluster sizing assumptions ----
            # Assume 32 total cores => choose 4 cores/executor => 8 executors (roughly)
            # These are often set in your cluster config:
            # .config("spark.executor.instances", "8")
            # .config("spark.executor.cores", "4")
            # .config("spark.executor.memory", "12g")
        

            # ---- Shuffle / AQE ----
            # Total cores = 32 => shuffle partitions ~ 2x cores
            .config("spark.sql.shuffle.partitions", "64")
            .config("spark.sql.adaptive.enabled", "true")

            # ---- Input file sizing ----
            # We have ~50GB input, we don't want 1000s of tiny splits.
            # 512MB per partition => ~ 100 input partitions.
            .config("spark.sql.files.maxPartitionBytes", 512 * 1024 * 1024)

            # ---- Broadcast threshold (optional) ----
            .config("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)

            .getOrCreate()
    )
