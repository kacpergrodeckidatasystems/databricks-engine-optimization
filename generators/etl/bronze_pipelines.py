# src/laboratory/ingestion_pipelines.py
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("BronzeETLGenerators")

class IngestionPipelines:
    def __init__(self, spark: SparkSession, source_volume_path: str):
        """
        Orkiestrator laboratoriów wydajnościowych.
        :param source_volume_path: Ścieżka do katalogu nadrzędnego (np. '/Volumes/workspace/default/labs')
        """
        self.spark = spark
        self.source_path = source_volume_path
        self.target_schema = "workspace.bronze"

    def run_etl_01_small_files(self):
        """ETL 1: Wymuszenie fizycznego rozproszenia danych na tabeli Delta."""
        logger.info("🚀 Uruchamianie ETL 1: Ingest telemetrii BESS (Small Files)...")
        raw_df = self.spark.read.json(f"{self.source_path}/bess_telemetry_raw")
        
        # ANTY-WZORZEC: Sztuczne rozerwanie małego zbioru na 500 plików
        bad_df = raw_df.repartition(500)
        
        target_table = f"{self.target_schema}.bess_telemetry_small_files"
        bad_df.write.mode("overwrite").format("delta").saveAsTable(target_table)
        logger.info(f"✅ ETL 1 zakończony. Tabela: {target_table}")

    def run_etl_02_type_casting_nightmare(self):
        """ETL 2: Degradacja silnych typów na STRING (Zabójstwo Data Skipping)."""
        logger.info("🚀 Uruchamianie ETL 2: Ingest odczytów PV (Utrata Typowania)...")
        raw_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(f"{self.source_path}/pv_metrics_raw")
        
        # ANTY-WZORZEC: Masowe rzutowanie metryk numerycznych IoT na STRING
        bad_df = raw_df.select(
            F.col("station_id").cast("string"),
            F.col("temperature").cast("string").alias("str_temperature"),
            F.col("voltage").cast("string").alias("str_voltage"),
            F.col("timestamp").cast("string")
        )
        
        target_table = f"{self.target_schema}.pv_metrics_string_nightmare"
        bad_df.write.mode("overwrite").format("delta").saveAsTable(target_table)
        logger.info(f"✅ ETL 2 zakończony. Tabela: {target_table}")

    def run_etl_03_missed_broadcast(self):
        """ETL 3: Wymuszenie SortMergeJoin na małym słowniku stacji."""
        logger.info("🚀 Uruchamianie ETL 3: Rejestracja widoku z ukrytym SortMergeJoin...")
        
        # 1. Zapisujemy surową tabelę faktów (BESS)
        df_facts = self.spark.read.json(f"{self.source_path}/bess_telemetry_raw")
        df_facts.write.mode("overwrite").format("delta").saveAsTable(f"{self.target_schema}.bess_facts_raw")
        
        # 2. Tworzymy i zapisujemy mały słownik stacji
        mock_data = [("S001", "Warszawa_Centrum"), ("S002", "Brwinow_Zachod"), ("S003", "Grodzisk_Mazowiecki")]
        df_dim = self.spark.createDataFrame(mock_data, ["station_id", "station_name"])
        df_dim.write.mode("overwrite").format("delta").saveAsTable(f"{self.target_schema}.stations_dict")

        # ANTY-WZORZEC ARCHITEKTONICZNY: 
        # Zamiast zapisu statycznego, tworzymy WIDOK z jawnym wyłączeniem broadcastu w sesji.
        # Kiedy framework APM odpali EXPLAIN na tym widoku, silnik Catalyst wyrenderuje SortMergeJoin!
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self.target_schema}.enriched_telemetry_heavy_shuffle AS 
            SELECT /*+ MERGE(d) */ f.*, d.station_name 
            FROM {self.target_schema}.bess_facts_raw f
            INNER JOIN {self.target_schema}.stations_dict d ON f.station_id = d.station_id
        """)
        logger.info(f"✅ ETL 3 zakończony. Utworzono dynamiczny widok: {self.target_schema}.enriched_telemetry_heavy_shuffle")

    def run_etl_04_over_partitioning_and_skew(self):
        """ETL 4: Prowokowanie paraliżu Drivera (Over-Partitioning klastra katalogów)."""
        logger.info("🚀 Uruchamianie ETL 4: Logi inwerterów (Over-Partitioning)...")
        raw_df = self.spark.read.json(f"{self.source_path}/inverter_logs_raw")
        
        # ANTY-WZORZEC: Klucz partycji oparty o unikalny identyfikator połączony z minutą zapisu
        bad_df = raw_df.withColumn("bad_partition_key", 
            F.concat(F.col("inverter_id"), F.lit("_min_"), F.date_format(F.col("timestamp"), "mm"))
        )
        
        target_table = f"{self.target_schema}.inverter_logs_partition_nightmare"
        
        # Zapis z fizycznym podziałem na tysiące podkatalogów (partitionBy)
        bad_df.write.mode("overwrite").format("delta").partitionBy("bad_partition_key").saveAsTable(target_table)
        logger.info(f"✅ ETL 4 zakończony. Tabela: {target_table}")