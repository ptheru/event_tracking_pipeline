# event_tracking_pipeline

# ⚡ Event Tracking Pipeline — Optimized Spark ETL (Bronze → Silver → Gold)

This repository contains a **production-grade, fully optimized** Spark ETL pipeline 
for processing large-scale *event tracking* data.

It includes:

- Multi-hop architecture (Bronze → Silver → Gold)
- Adaptive Query Execution (AQE)
- Partition strategy (input splits, shuffle partitions, write partitions)
- Broadcast join optimization 
- File size tuning (~256 MB Parquet files)
- Coalesce optimization in Gold layer
- Dimension modeling (dim_user, dim_video, dim_session)
- Fact modeling (fact_events)
- Gold aggregations (DAU, Funnel)
- Cluster-aware tuning (executors, cores, memory)
