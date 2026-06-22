#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame

# 1. Automatic orchestration of project paths for WHL distribution
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Import core components of heuristic engine
from src.auditor.engine import PerformanceEngine
from src.readers.dataframe_reader import DataFrameExplainReader
from src.policies.policy_manager import PolicyManager
from src.context.environment_provider import EnvironmentProvider
from src.suggestions.remediation_engine import RemediationEngine
from src.finops.cost_translator import CostTranslator
from src.reporters.console_reporter import ConsoleReporter

# 3. Import complete package of 10 detectors (PERF-001 to PERF-010)
from src.rules.physical_rules import SmallFilesRule, MissedBroadcastRule, OverPartitioningRule, DataSkewRule, MissingOptimizationRule
from src.rules.query_rules import TypeCastingRule, CartesianProductRule, ExplodeAbuseRule, RedundantScanRule, NonVectorizedUdfRule

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("APM-Universal-Trigger")


class APMAutomatedOrchestrator:
    """
    Universal, hands-free entry point for APM framework.
    Automatically adapts to environment: Databricks Serverless (gRPC) vs Vanilla Spark (Airflow/EMR).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.policy_manager = PolicyManager()
        self.env_provider = EnvironmentProvider(spark)
        self.remediation_engine = RemediationEngine()
        self.cost_translator = CostTranslator(self.policy_manager.get_policy("finops"))
        self.reporter = ConsoleReporter()

        # Complete test matrix ready for commercial distribution
        self.active_rules = [
            SmallFilesRule(),          # PERF-001
            MissedBroadcastRule(),     # PERF-002
            TypeCastingRule(),         # PERF-003
            OverPartitioningRule(),    # PERF-004
            CartesianProductRule(),    # PERF-005
            DataSkewRule(),            # PERF-006
            MissingOptimizationRule(), # PERF-007
            ExplodeAbuseRule(),        # PERF-008
            RedundantScanRule(),       # PERF-009
            NonVectorizedUdfRule()     # PERF-010
        ]

    def _safe_conf_get(self, key: str, default_value: str) -> str:
        """Defensive configuration read – resilient to gRPC Serverless blocks."""
        try:
            return self.spark.conf.get(key, default_value)
        except Exception:
            return default_value

    def _detect_runtime_platform(self) -> str:
        """
        Heuristically identifies runtime platform.
        Returns 'databricks' or 'vanilla_spark'.
        """
        # Check presence of Databricks-specific keys in runtime
        if self._safe_conf_get("spark.databricks.clusterUsageTags.clusterId", None) is not None:
            return "databricks"
        if "databricks" in self._safe_conf_get("spark.sql.warehouse.dir", "").lower():
            return "databricks"
        
        # Test Serverless-specific gRPC Connect environment
        try:
            if hasattr(self.spark, "client") or "connect" in str(type(self.spark)).lower():
                return "databricks"
        except Exception:
            pass
            
        return "vanilla_spark"

    def _discover_active_context(self, platform: str) -> Dict[str, Any]:
        """Retrieve session metadata in a platform-safe manner."""
        logger.info(f"🔍 [AUTODISCOVERY] Detected platform: {platform.upper()}. Mapping context...")
        
        context_meta = {"platform": platform, "catalog": "spark_catalog", "database": "default"}
        
        if platform == "databricks":
            try:
                context_meta["catalog"] = self.spark.catalog.currentCatalog()
                context_meta["database"] = self.spark.catalog.currentDatabase()
            except Exception:
                pass
            context_meta["aqe_enabled"] = self._safe_conf_get("spark.sql.adaptive.enabled", "true")
            context_meta["user"] = self._safe_conf_get("spark.databricks.clusterUsageTags.owner", "job_coordinator")
        else:
            # Profile for vanilla Spark (e.g., under Airflow / EMR / Apache Spark Standalone)
            try:
                context_meta["database"] = self.spark.catalog.currentDatabase()
            except Exception:
                pass
            context_meta["aqe_enabled"] = self._safe_conf_get("spark.sql.adaptive.enabled", "true")
            context_meta["user"] = os.getenv("USER", "airflow_operator")
            
        return context_meta

    def _get_tables_to_scan(self, platform: str, current_db: str) -> List[str]:
        """List database resources available for automatic inspection."""
        try:
            tables_df = self.spark.catalog.listTables(current_db)
            return [f"{current_db}.{t.name}" for t in tables_df if not t.isTemporary]
        except Exception as e:
            logger.warning(f"Unable to read table list for database '{current_db}': {str(e)}")
            return []

    def run_smart_scan(self, target_table: Optional[str] = None, df: Optional[DataFrame] = None) -> Dict[str, Any]:
        """
        Main scanner execution loop.
        Only requires initialized orchestrator object.
        """
        platform = self._detect_runtime_platform()
        meta = self._discover_active_context(platform)
        
        # Determine audit targets
        targets = []
        if df is not None:
            # Inline scenario: direct examination of DataFrame on the fly
            targets.append(("DataFrame_AdHoc", df))
        elif target_table:
            # Specific paginated table indicated
            try:
                targets.append((target_table, self.spark.read.table(target_table)))
            except Exception as e:
                logger.error(f"Skipped target {target_table}. Load error: {str(e)}")
        else:
            # Full automatic scan of entire available namespace
            logger.info(f"🤖 Starting automatic inventory and schema scan: {meta['database']}")
            found_tables = self._get_tables_to_scan(platform, meta['database'])
            for t_name in found_tables:
                try:
                    targets.append((t_name, self.spark.read.table(t_name)))
                except Exception:
                    continue

        if not targets:
            logger.warning("⚠️  [APM] No objects (tables/views/df) located for audit.")
            return {"status": "SKIPPED", "scanned": 0}

        # Orchestration and execution of test series
        scanned_count = 0
        for name, target_df in targets:
            context_label = f"APM-Scan-{name.replace('.', '_')}"
            logger.info(f"🎬 [RUN] Analyzing process for 10 anomalies: {name}")
            
            try:
                # Dynamic assignment of laboratory paths for PERF-001 rule
                discovered_volume = None
                if platform == "databricks" and "bess" in name.lower():
                    discovered_volume = "/Volumes/workspace/default/weather_data/raw_source/bess_telemetry_raw"

                # Initialize Catalyst plan reader
                reader = DataFrameExplainReader(
                    spark=self.spark,
                    df=target_df,
                    table_name=name if name != "DataFrame_AdHoc" else None,
                    source_volume_path=discovered_volume
                )

                engine = PerformanceEngine(
                    reader=reader,
                    rules=self.active_rules,
                    policy_manager=self.policy_manager,
                    env_provider=self.env_provider,
                    remediation_engine=self.remediation_engine,
                    cost_translator=self.cost_translator,
                    reporter=self.reporter
                )

                engine.run_audit(context_name=context_label)
                scanned_count += 1
                
            except Exception as e:
                logger.error(f"❌ Exception during process audit {name}: {str(e)}")

        return {"status": "FINISHED", "scanned_objects": scanned_count}


if __name__ == "__main__":
    # Production block for Databricks Workflows / Airflow BashOperator
    try:
        spark_session = SparkSession.builder.getOrCreate()
        orchestrator = APMAutomatedOrchestrator(spark_session)
        orchestrator.run_smart_scan()
    except Exception as e:
        logger.critical(f"Critical CLI initialization error: {str(e)}")
        sys.exit(1)
