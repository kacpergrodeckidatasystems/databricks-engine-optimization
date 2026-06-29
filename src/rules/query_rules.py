from src.auditor.models import IAnalysisRule, Alert
from typing import Dict, Any, Optional


class TypeCastingRule(IAnalysisRule):
    def evaluate(
        self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None
    ) -> Optional[Alert]:
        """Detects numeric IoT metrics degraded to STRING type."""
        detected_anomalies = []

        # 1. Structural check based on injected schema
        schema_fields = metrics.get("schema_fields", {})
        METRIC_KEYWORDS = [
            "temp",
            "temperature",
            "voltage",
            "soc",
            "battery",
            "current",
            "power",
            "value",
        ]

        for col_name, col_type in schema_fields.items():
            if col_type.lower() == "string":
                if any(keyword in col_name.lower() for keyword in METRIC_KEYWORDS):
                    detected_anomalies.append(f"metric '{col_name}' stored as STRING")

        # 2. Backup check for active CAST in text filters
        if plan_text:
            plan_lower = plan_text.lower()
            if "cast(" in plan_lower and any(
                k in plan_lower for k in ["filter", "datafilters", "photonfilter"]
            ):
                detected_anomalies.append("active type casting (CAST) inside WHERE condition")

        # Alert consolidation with explicit 'fix' parameter
        if detected_anomalies:
            return Alert(
                rule_id="PERF-003",
                title="Improper data typing (Type Casting & Metric Degradation)",
                description=f"Critical typing anomalies detected: {'; '.join(list(set(detected_anomalies)))}. "
                f"This completely blocks Delta Lake indexes (Data Skipping), forcing cluster to Full Table Scan.",
                severity="WARNING",
                fix="Ensure literal type in filters matches column type. Rebuild Bronze table so IoT metrics use numeric types (INT/DOUBLE).",
            )

        return None


class CartesianProductRule(IAnalysisRule):
    def evaluate(
        self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None
    ) -> Optional[Alert]:
        """PERF-005: Detects memory-killing Cartesian products (Cross Joins)."""
        if not plan_text:
            return None

        plan_lower = plan_text.lower()
        # Look for traces of joins without binding condition
        if "cartesianproduct" in plan_lower or "broadcastnestedloopjoin" in plan_lower:
            return Alert(
                rule_id="PERF-005",
                title="Cartesian product detected",
                description="Catalyst engine was forced to perform join without condition (Cross Join). This causes n-fold row multiplication and risks immediate OutOfMemory (OOM) error on Executors.",
                severity="CRITICAL",
                fix="Check join conditions .join(). If CROSS JOIN operation is intentional, ensure smaller table is packed with broadcast() instruction.",
            )
        return None


class ExplodeAbuseRule(IAnalysisRule):
    def evaluate(
        self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None
    ) -> Optional[Alert]:
        """PERF-008: Detects abuse of explode() function pumping RAM."""
        if not plan_text:
            return None

        if "generate explode(" in plan_text.lower():
            return Alert(
                rule_id="PERF-008",
                title="Inefficient structure explosion (Explode Abuse)",
                description="Use of explode() function forces physical row duplication for each array element. For large telemetry datasets, this causes sudden, avalanche-like increase in data volume in memory.",
                severity="WARNING",
                fix="Replace explode() function with native, vectorized higher-order functions for collections, such as transform(), filter(), or aggregate().",
            )
        return None


class RedundantScanRule(IAnalysisRule):
    def evaluate(
        self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None
    ) -> Optional[Alert]:
        """PERF-009: Detects multiple scans of same table in single query."""
        if not plan_text:
            return None

        table_name = metrics.get("schema_fields", {})  # Look for context name in plan text
        plan_lower = plan_text.lower()

        # If same physical Delta Scan appears in plan more than 2 times (no cache)
        scan_count = plan_lower.count("scan delta")
        if scan_count >= 3:
            return Alert(
                rule_id="PERF-009",
                title="Redundant Source Scanning (Redundant Table Scan)",
                description=f"Detected that same Delta Lake data source is read {scan_count} times within single pipeline. This results from multiple references to same DataFrame without state caching.",
                severity="WARNING",
                fix="Apply .cache() or .persist() operation on shared DataFrame before logic branching to read data from cloud storage only once.",
            )
        return None


class NonVectorizedUdfRule(IAnalysisRule):
    def evaluate(
        self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None
    ) -> Optional[Alert]:
        """PERF-010: Detects use of black boxes — generic Python UDFs."""
        if not plan_text:
            return None

        plan_lower = plan_text.lower()
        # Detect generic Python evaluation inside JVM
        if "batchevalpython" in plan_lower or "scalaudf" in plan_lower:
            return Alert(
                rule_id="PERF-010",
                title="Unverified UDF function usage (Row-by-Row Execution)",
                description="Standard Python UDF function detected. This disables processor vectorization (Photon Engine), forces Spark to continuously serialize data between JVM and Python interpreter, and processes data row by row.",
                severity="HIGH",
                fix="Replace custom Python code with built-in functions from pyspark.sql.functions package. If logic is too complex, rewrite as Pandas UDF (with PyArrow arrow typing).",
            )
        return None
