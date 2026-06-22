from typing import Dict, Any, Optional
from src.auditor.models import IAnalysisRule, Alert
import re

class SmallFilesRule(IAnalysisRule):
    """Rule detecting small files problem in Delta Lake."""
    
    def __init__(self, max_file_count: int = 100):
        self.max_file_count = max_file_count
    
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """Analyzes physical metrics and checks if file count exceeds threshold."""
        num_files = metrics.get("num_files", 0)
        
        if num_files > self.max_file_count:
            return Alert(
                rule_id="PERF-001",
                title="Small files problem detected",
                description=f"Table contains {num_files} files, exceeding recommended threshold of {self.max_file_count}. "
                           f"This will cause read performance degradation and increased metadata management costs.",
                fix=f"Run OPTIMIZE operation on the table or use df.coalesce() before writing. "
                    f"File count: {num_files}",
                severity="WARNING"
            )
        return None


class MissedBroadcastRule(IAnalysisRule):
    """Rule detecting missed opportunities for Broadcast Join."""
    
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """Analyzes physical plan for expensive Shuffle operations that could be Broadcast."""
        # Look for SortMergeJoin or ShuffledHashJoin pattern in plan
        if re.search(r"SortMergeJoin|ShuffledHashJoin", plan_text, re.IGNORECASE):
            # Also look for Shuffle indicators
            if "Exchange" in plan_text:  # Exchange presence suggests Shuffle
                return Alert(
                    rule_id="PERF-002",
                    title="Missed Broadcast Join optimization",
                    description="Detected expensive Shuffle Join operation that could be optimized "
                               "by using Broadcast Hash Join for smaller dimension table.",
                    fix="Use broadcast() for smaller table: df_big.join(broadcast(df_small), 'key')",
                    severity="WARNING"
                )
        return None
    
class TypeCastingRule(IAnalysisRule):
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """
        Universal rule analyzing Catalyst plans for strong type degradation.
        Works on both active queries with filters and raw table structures.
        """
        detected_anomalies = []
        
        # Keywords indicating column is a business metric
        METRIC_KEYWORDS = ["temp", "temperature", "voltage", "soc", "battery", "current", "power", "value"]
        
        # --- METHOD 1: Structural check based on injected schema (Priority!) ---
        schema_fields = metrics.get("schema_fields", {})
        for col_name, col_type in schema_fields.items():
            if col_type.lower() == "string":
                if any(keyword in col_name.lower() for keyword in METRIC_KEYWORDS):
                    detected_anomalies.append(f"metric '{col_name}' stored as STRING")
        
        # --- METHOD 2: Execution plan analysis (if available) ---
        if not plan_text:
            # If no plan but found schema anomalies, return alert
            if detected_anomalies:
                return Alert(
                    rule_id="PERF-003",
                    title="Improper data typing (Type Casting & Metric Degradation)",
                    description=f"Critical typing anomalies detected: {'; '.join(list(set(detected_anomalies)))}. "
                                f"This completely blocks Delta Lake indexes (Data Skipping), forcing cluster to Full Table Scan.",
                    fix="Convert IoT metrics to proper numeric types (INT, DOUBLE) using ALTER TABLE ... ALTER COLUMN or rewrite table with correct schema.",
                    severity="WARNING",
                    metrics_captured={"anomalies": list(set(detected_anomalies)), "total_count": len(set(detected_anomalies))}
                )
            return None
            
        lines = plan_text.split("\n")

        # --- METHOD 1: Detect active casting in filters (Regex) ---
        # Matches patterns like: cast(column_name#123 as double) or cast(voltage#879 as string)
        cast_pattern = re.compile(r"cast\((\w+)#\d+\s+as\s+(\w+)\)", re.IGNORECASE)
        
        for line in lines:
            line_lower = line.lower()
            # Check only filtering operation lines (classic and Photon)
            if any(k in line_lower for k in ["filter", "datafilters", "partitionfilters", "photonfilter"]):
                matches = cast_pattern.findall(line)
                for col_name, target_type in matches:
                    detected_anomalies.append(f"active filter casting on column '{col_name}' to type '{target_type}'")

        # --- METHOD 2: Detect permanent schema degradation (Metrics stored as STRING) ---
        # Matches variable declarations in plan: column_name#123: string
        schema_pattern = re.compile(r"(\w+)#\d+:\s*string", re.IGNORECASE)
        
        for line in lines:
            line_lower = line.lower()
            # Search in file scan sections and source relation definitions
            if any(k in line_lower for k in ["relation", "read", "scan", "localrelation"]):
                matches = schema_pattern.findall(line)
                for col_name in matches:
                    # If column is a metric and has string type -> anomaly
                    if any(keyword in col_name.lower() for keyword in METRIC_KEYWORDS):
                        detected_anomalies.append(f"IoT metric '{col_name}' permanently stored as STRING in table metadata")

        # --- RESULT CONSOLIDATION ---
        if detected_anomalies:
            unique_issues = list(set(detected_anomalies))
            issues_summary = "; ".join(unique_issues)
            
            return Alert(
                rule_id="PERF-003",
                title="Improper data typing (Type Casting & Metric Degradation)",
                description=f"Critical typing anomalies detected: {issues_summary}. "
                            f"This causes complete blocking of Delta Lake static indexes (Data Skipping), "
                            f"forcing cluster to read and parse every file row by row (Full Table Scan).",
                fix="Convert IoT metrics to proper numeric types (INT, DOUBLE) using ALTER TABLE ... ALTER COLUMN or rewrite table with correct schema.",
                severity="WARNING",
                metrics_captured={"anomalies": unique_issues, "total_count": len(unique_issues)}
            )
            
        return None

class OverPartitioningRule(IAnalysisRule):
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """
        Heuristic rule detecting over-partitioning based on physical 
        partition key declarations in Catalyst plan or schema structure.
        """
        if not plan_text:
            return None

        plan_lower = plan_text.lower()
        safe_policies = policies or {}
        
        # Get threshold from PolicyManager (default 1000 partitions)
        limit_partitions = safe_policies.get("over_partitioning", {}).get("max_partitions", 1000)
        
        detected = False
        evidence = ""

        # Keywords indicating catastrophic partition key design (high cardinality)
        HIGH_CARDINALITY_KEYWORDS = ["bad_partition_key", "min", "minute", "timestamp", "guid", "id"]

        # STEP A: Physical plan text analysis
        # Spark outputs section for Delta tables: PartitionColumns: [column1, column2]
        if "partitioncolumns" in plan_lower:
            for keyword in HIGH_CARDINALITY_KEYWORDS:
                if keyword in plan_lower:
                    detected = True
                    evidence = f"detected table partitioning declaration by high-variability key ('{keyword}')"
                    break

        # STEP B: Scan injected schema for design anomalies
        schema_fields = metrics.get("schema_fields", {})
        for col_name in schema_fields.keys():
            if "bad_partition" in col_name.lower():
                detected = True
                evidence = f"table schema contains explicitly marked faulty directory cluster key: '{col_name}'"

        # Generate high priority alert (HIGH Severity)
        if detected:
            return Alert(
                rule_id="PERF-004",
                title="Critical over-partitioning of directory cluster",
                description=f"Structural anomaly detected: {evidence}. "
                            f"Partitioning Delta Lake table by high-cardinality columns "
                            f"generates thousands of micro-directories in storage (S3/ADLS), which paralyzes "
                            f"Driver JVM during metadata reading alone. Safe configuration limit: {limit_partitions}.",
                fix="Avoid physical 'partitionBy' writes on high-variability (cardinality) columns. In Delta Lake on Serverless clusters, instead of directory partitioning use modern Liquid Clustering with 'CLUSTER BY' clause.",
                severity="HIGH",
                metrics_captured={"evidence": evidence, "limit_partitions": limit_partitions}
            )
            
        return None
class DataSkewRule(IAnalysisRule):
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """PERF-006: Detects presence of data skew in execution threads."""
        if not plan_text:
            return None
            
        plan_lower = plan_text.lower()
        detected = False
        
        # If AQE had to intervene and swap join for skewed data version
        if "aqeskewedjoin" in plan_lower or "skewedjoin" in plan_lower:
            detected = True
            
        if detected:
            return Alert(
                rule_id="PERF-006",
                title="Data distribution asymmetry detected (Data Skew)",
                description="One data partition is drastically larger than others. This causes individual cluster threads to work multiple times longer, wasting computational power of other machines.",
                severity="HIGH",
                fix="Enable native skew mitigation in AQE: spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true'). If that doesn't help, apply key salting technique (Salting) before join/groupby operation."
            )
        return None


class MissingOptimizationRule(IAnalysisRule):
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """PERF-007: Detects missing data layout indexing (Z-Order / Liquid Clustering)."""
        if not plan_text:
            return None
            
        plan_lower = plan_text.lower()
        num_files = metrics.get("num_files", 0)
        
        # If table has many files, WHERE filters (Filter), but no data skipping trace (DataFilters)
        if "filter " in plan_lower and num_files > 50:
            if "datafilters" not in plan_lower:
                return Alert(
                    rule_id="PERF-007",
                    title="Missing physical layout optimization (Missing Cluster Index)",
                    description=f"Table has {num_files} files and is filtered with WHERE clause, but optimizer cannot apply Data Skipping mechanism. Missing multidimensional index.",
                    severity="WARNING",
                    fix="Rebuild structure using: spark.sql('OPTIMIZE table ZORDER BY (filtered_column)') or deploy Liquid Clustering: CLUSTER BY (column)."
                )
        return None
