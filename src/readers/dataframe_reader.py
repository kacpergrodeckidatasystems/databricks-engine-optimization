import logging
from typing import Dict, Any
from src.auditor.models import IMetricsReader
from src.decorators import trace_execution, safe_execution
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("DataFrameExplainReader")

class DataFrameExplainReader(IMetricsReader):
    def __init__(self, spark: SparkSession, df: DataFrame = None, table_name: str = None, source_volume_path: str = None):
        """
        Defensywny czytnik metadanych obsługujący ścieżki tekstowe tabel chmurowych 
        oraz bezpośrednie instancje obiektów DataFrame in-memory.
        """
        self.spark = spark
        self.df = df
        self.table_name = table_name
        self.source_path = source_volume_path

    def _validate_and_resolve_target(self) -> bool:
        """Weryfikuje obecność obiektów źródłowych przed analizą."""
        if self.table_name:
            try:
                clean_name = self.table_name.replace("`", "")
                exists = self.spark.catalog.tableExists(clean_name)
                if not exists:
                    logger.error(f"[FAIL-FAST] Tabela '{clean_name}' nie istnieje w Unity Catalog.")
                    return False
                return True
            except Exception as e:
                logger.error(f"[FAIL-FAST] Wyjątek podczas sprawdzania metastore: {str(e)}")
                return False

        return self.df is not None

    @trace_execution
    @safe_execution(default_factory=str)
    def get_execution_plan(self) -> str:
        """Pobiera fizyczny plan wykonania Catalyst, rozróżniając typ źródła wejściowego."""
        if not self._validate_and_resolve_target():
            logger.warning("[AUDIT-SKIP] Brak prawidłowego źródła danych do wygenerowania planu.")
            return ""

        # ŚCIEŻKA A: Jeśli badamy fizyczną tabelę z Unity Catalog (np. Audyt 3 / Audyt 4)
        if self.table_name:
            clean_name = self.table_name.replace("`", "")
            plan_df = self.spark.sql(f"EXPLAIN EXTENDED SELECT * FROM {clean_name}")
            return plan_df.take(1)[0][0]
        
        # ŚCIEŻKA B: Jeśli badamy bezpośredni obiekt DataFrame przekazany w locie (np. Audyt 1)
        self.df.createOrReplaceTempView("temp_auditor_view")
        plan_df = self.spark.sql("EXPLAIN EXTENDED SELECT * FROM temp_auditor_view")
        return plan_df.take(1)[0][0]

    @trace_execution
    @safe_execution(default_factory=dict)
    def get_physical_metrics(self) -> Dict[str, Any]:
        """Agreguje metryki strukturalne ze schematu oraz wolumetrykę z katalogów fizycznych."""
        metrics = {"num_files": 0, "total_size_bytes": 0, "schema_fields": {}}
        
        # Leniwe wiązanie DataFrame na podstawie nazwy, jeśli obiekt df jest pusty
        if self.df is None and self.table_name:
            try:
                clean_name = self.table_name.replace("`", "")
                self.df = self.spark.read.table(clean_name)
            except Exception:
                pass

        if self.df is not None:
            try:
                metrics["schema_fields"] = {f.name: f.dataType.simpleString() for f in self.df.schema}
            except Exception as e:
                logger.warning(f"[METRICS-SKIP] Błąd mapowania schematu: {str(e)}")
        
        if self.source_path:
            try:
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(self.spark)
                files = [f for f in dbutils.fs.ls(self.source_path) if "_delta_log" not in f.path]
                metrics["num_files"] = len(files)
                metrics["total_size_bytes"] = sum([f.size for f in files])
            except Exception as e:
                logger.warning(f"[METRICS-SKIP] Błąd odczytu wolumetryki plików: {str(e)}")
                
        return metrics