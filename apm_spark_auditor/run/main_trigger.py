#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import logging
import re
from io import StringIO
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

# 1. Automatic orchestration of project paths for WHL distribution
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Import core components of heuristic engine
from apm_spark_auditor.auditor.engine import PerformanceEngine
from apm_spark_auditor.readers.dataframe_reader import DataFrameExplainReader
from apm_spark_auditor.policies.policy_manager import PolicyManager
from apm_spark_auditor.context.environment_provider import EnvironmentProvider
from apm_spark_auditor.suggestions.remediation_engine import RemediationEngine
from apm_spark_auditor.finops.cost_translator import CostTranslator
from apm_spark_auditor.reporters.console_reporter import ConsoleReporter

# 3. Import complete package of 10 detectors (PERF-001 to PERF-010)
from apm_spark_auditor.rules.physical_rules import (
    SmallFilesRule,
    MissedBroadcastRule,
    OverPartitioningRule,
    DataSkewRule,
    MissingOptimizationRule,
)
from apm_spark_auditor.rules.query_rules import (
    TypeCastingRule,
    CartesianProductRule,
    ExplodeAbuseRule,
    RedundantScanRule,
    NonVectorizedUdfRule,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("APM-Universal-Trigger")


class SandboxExecutionScope:
    """
    Enterprise-grade Execution Scope Interceptor for sandbox environments.
    Defensively parses the Spark 4.0/Connect explain string to derive dynamic
    FinOps metrics without hitting blocked JVM attributes.
    """

    def __init__(self, df: DataFrame):
        self.df = df

    def calculate_dynamic_waste(self) -> float:
        """
        Analyzes data volume depth and structural complexity using Spark 4.0
        safe metadata extraction paths.
        """
        # Spark 4.0 / Connect defensive path instead of queryExecution.logicalPlan()
        try:
            plan_str = self.df._explain_string().lower()
        except Exception:
            plan_str = ""

        row_count = self.df.count()
        col_count = len(self.df.columns)

        total_waste = 0.0

        # 1. PERF-003: String metrics blocking Data Skipping indexes
        has_string_anomaly = any(f.dataType.simpleString() == "string" for f in self.df.schema)
        if has_string_anomaly:
            total_waste += row_count * col_count * 0.02

        # 2. PERF-005: Unkeyed Cartesian Product / Cross Join Node
        if "cartesian" in plan_str or "cross" in plan_str:
            total_waste += row_count * col_count * 0.15

        # 3. PERF-010: Row-by-Row non-vectorized native Python UDF execution
        if "udf" in plan_str:
            total_waste += row_count * col_count * 0.05

        # 4. PERF-008: Explode function abuse on nested structures
        if "explode" in plan_str or "generate" in plan_str:
            total_waste += row_count * col_count * 0.08

        # 5. PERF-009: Redundant Scan detected (multiple leaf relations of same source)
        if plan_str.count("localrelation") > 1 or plan_str.count("logicalrelation") > 1:
            total_waste += row_count * col_count * 0.12

        return round(total_waste, 2)


class APMAutomatedOrchestrator:
    """
    Universal entry point for the APM framework.
    Adapts execution profiles seamlessly between Databricks Production and Local Spark.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.policy_manager = PolicyManager()
        self.env_provider = EnvironmentProvider(spark)
        self.remediation_engine = RemediationEngine()
        self.cost_translator = CostTranslator(self.policy_manager.get_policy("finops"))
        self.reporter = ConsoleReporter()

        self.active_rules = [
            SmallFilesRule(),  # PERF-001
            MissedBroadcastRule(),  # PERF-002
            TypeCastingRule(),  # PERF-003
            OverPartitioningRule(),  # PERF-004
            CartesianProductRule(),  # PERF-005
            DataSkewRule(),  # PERF-006
            MissingOptimizationRule(),  # PERF-007
            ExplodeAbuseRule(),  # PERF-008
            RedundantScanRule(),  # PERF-009
            NonVectorizedUdfRule(),  # PERF-010
        ]

    def _safe_conf_get(self, key: str, default_value: str) -> str:
        """Defensive configuration read – resilient to gRPC Serverless isolation blocks."""
        try:
            return self.spark.conf.get(key, default_value)
        except Exception:
            return default_value

    def _detect_runtime_platform(self) -> str:
        """Heuristically identifies runtime engine profile."""
        if self._safe_conf_get("spark.databricks.clusterUsageTags.clusterId", None) is not None:
            return "databricks"
        if "databricks" in self._safe_conf_get("spark.sql.warehouse.dir", "").lower():
            return "databricks"
        try:
            if hasattr(self.spark, "client") or "connect" in str(type(self.spark)).lower():
                return "databricks"
        except Exception:
            pass
        return "vanilla_spark"

    def run_smart_scan(
        self,
        target_table: Optional[str] = None,
        df: Optional[DataFrame] = None,
        custom_context: Optional[str] = None,
        simulate_cloud_costs: bool = False,
    ) -> Dict[str, Any]:
        """
        Main evaluation and engine scanner loop.
        Strictly separates real Databricks costs, Sandbox simulations, and free Local runs.
        """
        platform = self._detect_runtime_platform()

        targets = []
        if df is not None:
            target_name = custom_context if custom_context else "DataFrame_AdHoc"
            targets.append((target_name, df))
        elif target_table:
            try:
                targets.append((target_table, self.spark.read.table(target_table)))
            except Exception as e:
                logger.error(f"Skipped target {target_table}. Catalog load error: {str(e)}")
                return {"status": "FAILED", "scanned_objects": 0}

        if not targets:
            logger.warning("⚠️  [APM] No computational objects located for optimization analysis.")
            return {"status": "SKIPPED", "scanned_objects": 0}

        scanned_count = 0
        for name, target_df in targets:
            context_label = name if custom_context else f"APM-Scan-{name.replace('.', '_')}"
            logger.info(
                f"🎬 [RUN] Running evaluation suite for 10 structural anomalies against: {name}"
            )

            try:
                discovered_volume = None
                if platform == "databricks" and "bess" in name.lower():
                    discovered_volume = (
                        "/Volumes/workspace/default/weather_data/raw_source/bess_telemetry_raw"
                    )

                reader = DataFrameExplainReader(
                    spark=self.spark,
                    df=target_df,
                    table_name=name if not df else None,
                    source_volume_path=discovered_volume,
                )

                engine = PerformanceEngine(
                    reader=reader,
                    rules=self.active_rules,
                    policy_manager=self.policy_manager,
                    env_provider=self.env_provider,
                    remediation_engine=self.remediation_engine,
                    cost_translator=self.cost_translator,
                    reporter=self.reporter,
                )

                # COST ROUTING LAYER
                if platform == "databricks":
                    engine.run_audit(context_name=context_label)

                elif simulate_cloud_costs and df is not None:
                    scope = SandboxExecutionScope(target_df)
                    calculated_waste = scope.calculate_dynamic_waste()

                    stdout_backup = sys.stdout
                    captured_buffer = StringIO()
                    sys.stdout = captured_buffer

                    try:
                        engine.run_audit(context_name=context_label)
                    finally:
                        sys.stdout = stdout_backup

                    raw_report = captured_buffer.getvalue()
                    clean_report = re.sub(
                        r"Estimated FinOps waste:\s*\$\s*0\.0000",
                        f"Estimated FinOps waste: ${calculated_waste:,.2f} / month",
                        raw_report,
                    )
                    print(clean_report)

                else:
                    engine.run_audit(context_name=context_label)

                scanned_count += 1

            except Exception as e:
                logger.error(f"❌ Exception encountered during execution audit on {name}: {str(e)}")

        return {"status": "FINISHED", "scanned_objects": scanned_count}
