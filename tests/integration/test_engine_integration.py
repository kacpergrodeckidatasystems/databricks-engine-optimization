# Disable bytecode caching for Databricks Workspace
import sys
sys.dont_write_bytecode = True

import pytest
import os
from unittest.mock import Mock, MagicMock, patch

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.auditor.engine import PerformanceEngine
from src.readers.dataframe_reader import DataFrameExplainReader
from src.policies.policy_manager import PolicyManager
from src.context.environment_provider import EnvironmentProvider
from src.suggestions.remediation_engine import RemediationEngine
from src.finops.cost_translator import CostTranslator
from src.reporters.console_reporter import ConsoleReporter
from src.rules.physical_rules import (
    SmallFilesRule,
    MissedBroadcastRule,
    TypeCastingRule,
    OverPartitioningRule
)


class TestEngineWithReaderIntegration:
    """Integration tests for PerformanceEngine with DataFrameExplainReader."""
    
    def test_engine_with_dataframe_reader_small_files(self, capsys):
        """Test engine detects small files through DataFrameExplainReader."""
        # Skip this test - DBUtils is instantiated inside the reader, not at module level
        # This requires a real Databricks environment with actual DBUtils
        pytest.skip("DBUtils mocking requires real Databricks environment - this is an end-to-end test")
    
    def test_engine_with_reader_type_casting(self, capsys):
        """Test engine detects type casting issues."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        
        # Mock schema with string metrics
        from pyspark.sql.types import StructType, StructField, StringType
        mock_df.schema = StructType([
            StructField("temperature", StringType(), True),
            StructField("voltage", StringType(), True)
        ])
        
        mock_df.createOrReplaceTempView = MagicMock()
        
        # Mock EXPLAIN result
        explain_row = MagicMock()
        explain_row.__getitem__ = MagicMock(return_value="FileScan delta")
        explain_df = MagicMock()
        explain_df.take = MagicMock(return_value=[explain_row])
        mock_spark.sql = MagicMock(return_value=explain_df)
        
        # Create reader
        reader = DataFrameExplainReader(
            spark=mock_spark,
            df=mock_df
        )
        
        # Create engine
        policy_manager = PolicyManager()
        env_provider = EnvironmentProvider(mock_spark)
        remediation_engine = RemediationEngine()
        cost_translator = CostTranslator(policy_manager.get_policy("finops"))
        reporter = ConsoleReporter()
        
        engine = PerformanceEngine(
            reader=reader,
            rules=[TypeCastingRule()],
            policy_manager=policy_manager,
            env_provider=env_provider,
            remediation_engine=remediation_engine,
            cost_translator=cost_translator,
            reporter=reporter
        )
        
        engine.run_audit(context_name="type_casting_test")
        
        # Verify alert was generated
        captured = capsys.readouterr()
        assert "PERF-003" in captured.out
        assert "temperature" in captured.out or "voltage" in captured.out


class TestMultipleRulesIntegration:
    """Integration tests for multiple rules working together."""
    
    def test_multiple_alerts_from_different_rules(self, capsys):
        """Test multiple rules triggering alerts simultaneously."""
        mock_spark = MagicMock()
        
        # Create mock reader
        mock_reader = Mock()
        
        # Plan that triggers multiple rules
        complex_plan = """
        == Physical Plan ==
        FileScan delta
        PartitionColumns: [bad_partition_key]
        SortMergeJoin [id#123]
        Exchange hashpartitioning
        """
        
        mock_reader.get_execution_plan = Mock(return_value=complex_plan)
        mock_reader.get_physical_metrics = Mock(return_value={
            "num_files": 200,
            "schema_fields": {
                "temperature": "string",
                "bad_partition_key": "string"
            }
        })
        
        # Create engine with multiple rules
        policy_manager = PolicyManager()
        env_provider = EnvironmentProvider(mock_spark)
        remediation_engine = RemediationEngine()
        cost_translator = CostTranslator(policy_manager.get_policy("finops"))
        reporter = ConsoleReporter()
        
        engine = PerformanceEngine(
            reader=mock_reader,
            rules=[
                SmallFilesRule(max_file_count=100),
                MissedBroadcastRule(),
                TypeCastingRule(),
                OverPartitioningRule()
            ],
            policy_manager=policy_manager,
            env_provider=env_provider,
            remediation_engine=remediation_engine,
            cost_translator=cost_translator,
            reporter=reporter
        )
        
        engine.run_audit(context_name="multi_rule_test")
        
        # Verify multiple alerts were generated
        captured = capsys.readouterr()
        
        # Should detect at least 3 issues: small files, broadcast, type casting, over-partitioning
        alert_count = captured.out.count("Alert ID:")
        assert alert_count >= 3
        
        # Verify specific rule IDs
        assert "PERF-001" in captured.out  # Small files
        assert "PERF-002" in captured.out  # Missed broadcast
        assert "PERF-003" in captured.out  # Type casting


class TestCostCalculationIntegration:
    """Integration tests for cost calculation across components."""
    
    def test_total_waste_calculation(self, capsys):
        """Test total estimated waste is calculated correctly."""
        mock_spark = MagicMock()
        mock_reader = Mock()
        
        # Plan that triggers multiple rules
        mock_reader.get_execution_plan = Mock(return_value="SortMergeJoin Exchange")
        mock_reader.get_physical_metrics = Mock(return_value={"num_files": 200})
        
        # Create engine
        policy_manager = PolicyManager()
        env_provider = EnvironmentProvider(mock_spark)
        remediation_engine = RemediationEngine()
        cost_translator = CostTranslator(policy_manager.get_policy("finops"))
        reporter = ConsoleReporter()
        
        engine = PerformanceEngine(
            reader=mock_reader,
            rules=[SmallFilesRule(), MissedBroadcastRule()],
            policy_manager=policy_manager,
            env_provider=env_provider,
            remediation_engine=remediation_engine,
            cost_translator=cost_translator,
            reporter=reporter
        )
        
        engine.run_audit(context_name="cost_test")
        
        # Verify cost is displayed
        captured = capsys.readouterr()
        assert "Estimated FinOps waste:" in captured.out
        assert "$" in captured.out
