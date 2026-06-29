import logging
from typing import Dict, Any
from apm_spark_auditor.auditor.models import IMetricsReader
from apm_spark_auditor.decorators import trace_execution, safe_execution
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("DataFrameExplainReader")


class DataFrameExplainReader(IMetricsReader):
    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame = None,
        table_name: str = None,
        source_volume_path: str = None,
    ):
        """
        Defensive metadata reader supporting cloud table text paths
        and direct in-memory DataFrame object instances.
        """
        self.spark = spark
        self.df = df
        self.table_name = table_name
        self.source_path = source_volume_path

    def _validate_and_resolve_target(self) -> bool:
        """Verifies presence of source objects before analysis."""
        if self.table_name:
            try:
                clean_name = self.table_name.replace("`", "")
                exists = self.spark.catalog.tableExists(clean_name)
                if not exists:
                    logger.error(
                        f"[FAIL-FAST] Table '{clean_name}' does not exist in Unity Catalog."
                    )
                    return False
                return True
            except Exception as e:
                logger.error(f"[FAIL-FAST] Exception during metastore check: {str(e)}")
                return False

        return self.df is not None

    @trace_execution
    @safe_execution(default_factory=str)
    def get_execution_plan(self) -> str:
        """Retrieves Catalyst physical execution plan, distinguishing input source type."""
        if not self._validate_and_resolve_target():
            logger.warning("[AUDIT-SKIP] No valid data source to generate plan.")
            return ""

        # PATH A: If examining physical table from Unity Catalog (e.g., Audit 3 / Audit 4)
        if self.table_name:
            clean_name = self.table_name.replace("`", "")
            plan_df = self.spark.sql(f"EXPLAIN EXTENDED SELECT * FROM {clean_name}")
            return plan_df.take(1)[0][0]

        # PATH B: If examining direct DataFrame object passed on the fly (e.g., Audit 1)
        self.df.createOrReplaceTempView("temp_auditor_view")
        plan_df = self.spark.sql("EXPLAIN EXTENDED SELECT * FROM temp_auditor_view")
        return plan_df.take(1)[0][0]

    @trace_execution
    @safe_execution(default_factory=dict)
    def get_physical_metrics(self) -> Dict[str, Any]:
        """Aggregates structural metrics from schema and volumetrics from physical directories."""
        metrics = {"num_files": 0, "total_size_bytes": 0, "schema_fields": {}}

        # Lazy DataFrame binding based on name, if df object is empty
        if self.df is None and self.table_name:
            try:
                clean_name = self.table_name.replace("`", "")
                self.df = self.spark.read.table(clean_name)
            except Exception:
                pass

        if self.df is not None:
            try:
                metrics["schema_fields"] = {
                    f.name: f.dataType.simpleString() for f in self.df.schema
                }
            except Exception as e:
                logger.warning(f"[METRICS-SKIP] Schema mapping error: {str(e)}")

        if self.source_path:
            try:
                from pyspark.dbutils import DBUtils

                dbutils = DBUtils(self.spark)
                files = [f for f in dbutils.fs.ls(self.source_path) if "_delta_log" not in f.path]
                metrics["num_files"] = len(files)
                metrics["total_size_bytes"] = sum([f.size for f in files])
            except Exception as e:
                logger.warning(f"[METRICS-SKIP] File volumetrics read error: {str(e)}")

        return metrics
