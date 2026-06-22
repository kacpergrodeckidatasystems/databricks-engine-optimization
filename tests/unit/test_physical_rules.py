# Disable bytecode caching for Databricks Workspace
import sys
sys.dont_write_bytecode = True

import pytest
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.rules.physical_rules import (
    SmallFilesRule,
    MissedBroadcastRule,
    TypeCastingRule,
    OverPartitioningRule,
    DataSkewRule,
    MissingOptimizationRule
)


class TestSmallFilesRule:
    """Unit tests for SmallFilesRule."""
    
    def test_small_files_detected(self, sample_metrics, sample_policies):
        """Test detection of small files problem."""
        rule = SmallFilesRule(max_file_count=100)
        metrics = {"num_files": 200}
        
        alert = rule.evaluate("", metrics, sample_policies)
        
        assert alert is not None
        assert alert.rule_id == "PERF-001"
        assert "200 files" in alert.description
        assert alert.severity == "WARNING"
    
    def test_small_files_not_detected(self, sample_metrics, sample_policies):
        """Test no alert when files count is below threshold."""
        rule = SmallFilesRule(max_file_count=100)
        metrics = {"num_files": 50}
        
        alert = rule.evaluate("", metrics, sample_policies)
        
        assert alert is None
    
    def test_custom_threshold(self):
        """Test custom file count threshold."""
        rule = SmallFilesRule(max_file_count=500)
        metrics = {"num_files": 450}
        
        alert = rule.evaluate("", metrics)
        
        assert alert is None


class TestMissedBroadcastRule:
    """Unit tests for MissedBroadcastRule."""
    
    def test_sortmergejoin_detected(self):
        """Test detection of SortMergeJoin with Exchange."""
        rule = MissedBroadcastRule()
        plan = """
        == Physical Plan ==
        SortMergeJoin [id#123], [station_id#456]
        :- Exchange hashpartitioning
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-002"
        assert "Broadcast" in alert.title
        assert alert.severity == "WARNING"
    
    def test_shuffled_hash_join_detected(self):
        """Test detection of ShuffledHashJoin."""
        rule = MissedBroadcastRule()
        plan = """
        ShuffledHashJoin [id#123]
        Exchange hashpartitioning
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-002"
    
    def test_no_shuffle_join(self):
        """Test no alert when no shuffle join present."""
        rule = MissedBroadcastRule()
        plan = """
        BroadcastHashJoin [id#123]
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is None


class TestTypeCastingRule:
    """Unit tests for TypeCastingRule."""
    
    def test_string_metric_detected_in_schema(self):
        """Test detection of metrics stored as STRING."""
        rule = TypeCastingRule()
        metrics = {
            "schema_fields": {
                "temperature": "string",
                "voltage": "string",
                "id": "int"
            }
        }
        
        alert = rule.evaluate("", metrics)
        
        assert alert is not None
        assert alert.rule_id == "PERF-003"
        assert "temperature" in alert.description or "voltage" in alert.description
        assert alert.severity == "WARNING"
    
    def test_cast_in_filter_detected(self):
        """Test detection of CAST in filter conditions."""
        rule = TypeCastingRule()
        plan = """
        Filter (cast(temperature#123 as double) > 25.0)
        """
        metrics = {"schema_fields": {}}
        
        alert = rule.evaluate(plan, metrics)
        
        assert alert is not None
        assert "casting" in alert.description.lower()
    
    def test_correct_types_no_alert(self):
        """Test no alert when types are correct."""
        rule = TypeCastingRule()
        metrics = {
            "schema_fields": {
                "temperature": "double",
                "voltage": "double",
                "id": "int"
            }
        }
        
        alert = rule.evaluate("", metrics)
        
        assert alert is None


class TestOverPartitioningRule:
    """Unit tests for OverPartitioningRule."""
    
    def test_bad_partition_key_detected(self):
        """Test detection of high-cardinality partition key."""
        rule = OverPartitioningRule()
        plan = """
        PartitionColumns: [bad_partition_key, timestamp]
        """
        metrics = {"schema_fields": {}}
        policies = {"over_partitioning": {"max_partitions": 1000}}
        
        alert = rule.evaluate(plan, metrics, policies)
        
        assert alert is not None
        assert alert.rule_id == "PERF-004"
        assert alert.severity == "HIGH"
    
    def test_timestamp_partition_detected(self):
        """Test detection of timestamp-based partitioning."""
        rule = OverPartitioningRule()
        plan = """
        PartitionColumns: [timestamp, minute]
        """
        metrics = {}
        
        alert = rule.evaluate(plan, metrics)
        
        assert alert is not None
        # Description contains ('min') or ('timestamp') - check for these patterns
        desc_lower = alert.description.lower()
        assert "'min'" in desc_lower or "'timestamp'" in desc_lower or "'minute'" in desc_lower or "variability" in desc_lower
    
    def test_schema_field_bad_partition(self):
        """Test detection via schema field names."""
        rule = OverPartitioningRule()
        plan = "partitioncolumns present"
        metrics = {
            "schema_fields": {
                "bad_partition_key": "string",
                "data": "int"
            }
        }
        
        alert = rule.evaluate(plan, metrics)
        
        assert alert is not None
    
    def test_good_partitioning_no_alert(self):
        """Test no alert for proper partitioning."""
        rule = OverPartitioningRule()
        plan = """
        PartitionColumns: [year, month]
        """
        metrics = {}
        
        alert = rule.evaluate(plan, metrics)
        
        assert alert is None


class TestDataSkewRule:
    """Unit tests for DataSkewRule."""
    
    def test_aqe_skewed_join_detected(self):
        """Test detection of AQE skewed join intervention."""
        rule = DataSkewRule()
        plan = """
        AQESkewedJoin [id#123]
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-006"
        assert "Skew" in alert.title
        assert alert.severity == "HIGH"
    
    def test_skewed_join_detected(self):
        """Test detection of SkewedJoin."""
        rule = DataSkewRule()
        plan = "SkewedJoin operation"
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
    
    def test_no_skew_no_alert(self):
        """Test no alert when no skew detected."""
        rule = DataSkewRule()
        plan = "BroadcastHashJoin [id#123]"
        
        alert = rule.evaluate(plan, {})
        
        assert alert is None


class TestMissingOptimizationRule:
    """Unit tests for MissingOptimizationRule."""
    
    def test_missing_data_skipping(self):
        """Test detection of missing data skipping optimization."""
        rule = MissingOptimizationRule()
        plan = """
        Filter (temperature > 25.0)
        FileScan delta
        """
        metrics = {"num_files": 100}
        
        alert = rule.evaluate(plan, metrics)
        
        assert alert is not None
        assert alert.rule_id == "PERF-007"
        assert "skipping" in alert.description.lower() or "index" in alert.description.lower()
    
    def test_with_data_filters_no_alert(self):
        """Test no alert when DataFilters present."""
        rule = MissingOptimizationRule()
        plan = """
        Filter (temperature > 25.0)
        DataFilters: [temperature > 25.0]
        FileScan delta
        """
        metrics = {"num_files": 100}
        
        alert = rule.evaluate(plan, metrics)
        
        assert alert is None
    
    def test_few_files_no_alert(self):
        """Test no alert when file count is low."""
        rule = MissingOptimizationRule()
        plan = "Filter (temperature > 25.0)"
        metrics = {"num_files": 10}
        
        alert = rule.evaluate(plan, metrics)
        
        assert alert is None
