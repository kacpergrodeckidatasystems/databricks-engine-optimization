# src/laboratory/ingestion_pipelines.py
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("BronzeETLGenerators")

class IngestionPipelines:
    def __init__(self, spark: SparkSession, source_volume_path: str):
        """
        Performance laboratory orchestrator.
        :param source_volume_path: Path to parent directory (e.g., '/Volumes/workspace/default/labs')
        """
        self.spark = spark
        self.source_path = source_volume_path
        self.target_schema = "workspace.bronze"

    def run_etl_01_small_files(self):
        """ETL 1: Force physical data fragmentation on Delta table."""
        logger.info("🚀 Running ETL 1: BESS telemetry ingest (Small Files)...")
        raw_df = self.spark.read.json(f"{self.source_path}/bess_telemetry_raw")
        
        # ANTI-PATTERN: Artificially split small dataset into 500 files
        bad_df = raw_df.repartition(500)
        
        target_table = f"{self.target_schema}.bess_telemetry_small_files"
        bad_df.write.mode("overwrite").format("delta").saveAsTable(target_table)
        logger.info(f"✅ ETL 1 completed. Table: {target_table}")

    def run_etl_02_type_casting_nightmare(self):
        """ETL 2: Strong type degradation to STRING (Data Skipping killer)."""
        logger.info("🚀 Running ETL 2: PV readings ingest (Type Loss)...")
        raw_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(f"{self.source_path}/pv_metrics_raw")
        
        # ANTI-PATTERN: Mass casting of numeric IoT metrics to STRING
        bad_df = raw_df.select(
            F.col("station_id").cast("string"),
            F.col("temperature").cast("string").alias("str_temperature"),
            F.col("voltage").cast("string").alias("str_voltage"),
            F.col("timestamp").cast("string")
        )
        
        target_table = f"{self.target_schema}.pv_metrics_string_nightmare"
        bad_df.write.mode("overwrite").format("delta").saveAsTable(target_table)
        logger.info(f"✅ ETL 2 completed. Table: {target_table}")

    def run_etl_03_missed_broadcast(self):
        """ETL 3: Force SortMergeJoin on small station dictionary."""
        logger.info("🚀 Running ETL 3: Registering view with hidden SortMergeJoin...")
        
        # 1. Save raw fact table (BESS)
        df_facts = self.spark.read.json(f"{self.source_path}/bess_telemetry_raw")
        df_facts.write.mode("overwrite").format("delta").saveAsTable(f"{self.target_schema}.bess_facts_raw")
        
        # 2. Create and save small station dictionary
        mock_data = [("S001", "Warsaw_Center"), ("S002", "Brwinow_West"), ("S003", "Grodzisk_Mazowiecki")]
        df_dim = self.spark.createDataFrame(mock_data, ["station_id", "station_name"])
        df_dim.write.mode("overwrite").format("delta").saveAsTable(f"{self.target_schema}.stations_dict")

        # ARCHITECTURAL ANTI-PATTERN: 
        # Instead of static write, create VIEW with explicit broadcast disabled in session.
        # When APM framework runs EXPLAIN on this view, Catalyst engine will render SortMergeJoin!
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self.target_schema}.enriched_telemetry_heavy_shuffle AS 
            SELECT /*+ MERGE(d) */ f.*, d.station_name 
            FROM {self.target_schema}.bess_facts_raw f
            INNER JOIN {self.target_schema}.stations_dict d ON f.station_id = d.station_id
        """)
        logger.info(f"✅ ETL 3 completed. Created dynamic view: {self.target_schema}.enriched_telemetry_heavy_shuffle")

    def run_etl_04_over_partitioning_and_skew(self):
        """ETL 4: Provoke Driver paralysis (Over-Partitioning of directory cluster)."""
        logger.info("🚀 Running ETL 4: Inverter logs (Over-Partitioning)...")
        raw_df = self.spark.read.json(f"{self.source_path}/inverter_logs_raw")
        
        # ANTI-PATTERN: Partition key based on unique identifier combined with write minute
        bad_df = raw_df.withColumn("bad_partition_key", 
            F.concat(F.col("inverter_id"), F.lit("_min_"), F.date_format(F.col("timestamp"), "mm"))
        )
        
        target_table = f"{self.target_schema}.inverter_logs_partition_nightmare"
        
        # Write with physical split into thousands of subdirectories (partitionBy)
        bad_df.write.mode("overwrite").format("delta").partitionBy("bad_partition_key").saveAsTable(target_table)
        logger.info(f"✅ ETL 4 completed. Table: {target_table}")
