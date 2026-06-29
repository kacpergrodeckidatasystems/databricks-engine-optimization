from src.auditor.models import IMetricsReader
from src.decorators import trace_execution, safe_execution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from typing import Dict, Any


class EventLogReader(IMetricsReader):
    def __init__(self, spark: SparkSession, event_log_dir: str):
        self.spark = spark
        self.event_log_dir = event_log_dir

    @trace_execution
    @safe_execution(default_factory=str)
    def get_execution_plan(self) -> str:
        logs_df = self.spark.read.json(self.event_log_dir)
        plan_row = (
            logs_df.filter(F.col("Event") == "SparkListenerSQLExecutionStart")
            .select("physicalPlanDescription")
            .orderBy(F.col("time").desc())
            .first()
        )
        return (
            plan_row["physicalPlanDescription"]
            if plan_row and plan_row["physicalPlanDescription"]
            else ""
        )

    @trace_execution
    @safe_execution(default_factory=dict)
    def get_physical_metrics(self) -> Dict[str, Any]:
        logs_df = self.spark.read.json(self.event_log_dir)
        task_ends = logs_df.filter(F.col("Event") == "SparkListenerTaskEnd")

        metrics = {
            "max_task_duration_ms": 0,
            "min_task_duration_ms": 0,
            "num_files": 0,
            "total_size_bytes": 0,
        }

        if task_ends.count() > 0:
            stats = task_ends.select(
                F.max("Task Info.Duration").alias("max_duration"),
                F.min("Task Info.Duration").alias("min_duration"),
            ).first()
            metrics["max_task_duration_ms"] = stats["max_duration"] or 0
            metrics["min_task_duration_ms"] = stats["min_duration"] or 0

        return metrics
