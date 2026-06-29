#!/usr/bin/env python3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from src.run.main_trigger import APMAutomatedOrchestrator

# 1. Start local Spark session
spark = (
    SparkSession.builder.master("local[*]")
    .appName("APM-Enterprise-Multi-Anomaly-Demo")
    .getOrCreate()
)

# ANOMALY 1 (PERF-003): Core numeric metrics stored as STRING
base_data = [("1", "24.5", "3.2", "2026-06-29 08:00:00")] * 500
schema = ["id", "temperature", "voltage", "timestamp"]
df_base = spark.createDataFrame(base_data, schema)

# ANOMALY 2 (PERF-010): Use of standard, non-vectorized Python UDF
slow_custom_udf = F.udf(lambda val: f"BESS_NODE_{val}", StringType())
df_udf = df_base.withColumn("device_status", slow_custom_udf(F.col("id")))

# ANOMALY 3 (PERF-008): Explode operation abuse on dynamically generated array
df_exploded = df_udf.withColumn("matrix_factor", F.explode(F.array(F.lit("ALPHA"), F.lit("BETA"))))

# ANOMALY 4 (PERF-009): Redundant Scan - Self-join to source without checkpoint/cache
df_redundant = df_exploded.join(df_base, on="id", how="inner")

# ANOMALY 5 (PERF-005): Cartesian product (Unkeyed Cross Join) at the end of pipeline
df_meta = spark.createDataFrame([("Main_Cluster",)], ["cluster_name"])
df_final = df_redundant.crossJoin(df_meta)

# 2. Run audit through the official, clean framework API
orchestrator = APMAutomatedOrchestrator(spark)
result = orchestrator.run_smart_scan(
    df=df_final,
    custom_context="enterprise_bess_heavy_telemetry_pipeline",
    simulate_cloud_costs=True,
)

print(f"\nExecution Status: {result}")
