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

from src.run.main_trigger import APMAutomatedOrchestrator


@pytest.mark.system
class TestAPMOrchestratorEndToEnd:
    """System-level end-to-end tests for APM Orchestrator."""
    
    def test_orchestrator_initialization(self):
        """Test orchestrator initializes all components correctly."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(return_value="true")
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        
        assert orchestrator is not None
        assert orchestrator.spark == mock_spark
        assert orchestrator.policy_manager is not None
        assert orchestrator.env_provider is not None
        assert orchestrator.remediation_engine is not None
        assert orchestrator.cost_translator is not None
        assert orchestrator.reporter is not None
        assert len(orchestrator.active_rules) == 10  # All 10 rules loaded
    
    def test_platform_detection_databricks(self):
        """Test platform detection for Databricks."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(side_effect=lambda key, default: {
            "spark.databricks.clusterUsageTags.clusterId": "cluster-123",
            "spark.sql.warehouse.dir": "dbfs:/user/hive/warehouse"
        }.get(key, default))
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        platform = orchestrator._detect_runtime_platform()
        
        assert platform == "databricks"
    
    def test_platform_detection_vanilla_spark(self):
        """Test platform detection for vanilla Spark."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(return_value="default")
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        platform = orchestrator._detect_runtime_platform()
        
        assert platform == "vanilla_spark"
    
    def test_context_discovery_databricks(self):
        """Test context discovery on Databricks platform."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(side_effect=lambda key, default: {
            "spark.sql.adaptive.enabled": "true",
            "spark.databricks.clusterUsageTags.owner": "test_user"
        }.get(key, default))
        mock_spark.catalog.currentCatalog = MagicMock(return_value="main")
        mock_spark.catalog.currentDatabase = MagicMock(return_value="default")
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        context = orchestrator._discover_active_context("databricks")
        
        assert context["platform"] == "databricks"
        assert context["catalog"] == "main"
        assert context["database"] == "default"
        assert context["aqe_enabled"] == "true"
        assert context["user"] == "test_user"
    
    def test_smart_scan_with_dataframe(self, capsys):
        """Test smart scan with inline DataFrame."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(return_value="true")
        
        # Create mock DataFrame with schema
        mock_df = MagicMock()
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
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        result = orchestrator.run_smart_scan(df=mock_df)
        
        assert result["status"] == "FINISHED"
        assert result["scanned_objects"] == 1
        
        # Verify output
        captured = capsys.readouterr()
        assert "APM CORE REPORT" in captured.out
    
    def test_smart_scan_with_table_name(self):
        """Test smart scan with specific table name."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(return_value="true")
        mock_spark.catalog.currentDatabase = MagicMock(return_value="default")
        
        # Mock table read
        mock_df = MagicMock()
        from pyspark.sql.types import StructType, StructField, IntegerType
        mock_df.schema = StructType([StructField("id", IntegerType(), True)])
        mock_df.createOrReplaceTempView = MagicMock()
        mock_spark.read.table = MagicMock(return_value=mock_df)
        
        # Mock EXPLAIN result
        explain_row = MagicMock()
        explain_row.__getitem__ = MagicMock(return_value="FileScan delta")
        explain_df = MagicMock()
        explain_df.take = MagicMock(return_value=[explain_row])
        mock_spark.sql = MagicMock(return_value=explain_df)
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        result = orchestrator.run_smart_scan(target_table="test_table")
        
        assert result["status"] == "FINISHED"
        mock_spark.read.table.assert_called_once_with("test_table")
    
    def test_smart_scan_auto_discovery(self):
        """Test smart scan with automatic table discovery."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(return_value="true")
        mock_spark.catalog.currentCatalog = MagicMock(return_value="main")
        mock_spark.catalog.currentDatabase = MagicMock(return_value="bronze")
        
        # Mock table listing
        class MockTable:
            def __init__(self, name, is_temp):
                self.name = name
                self.isTemporary = is_temp
        
        mock_tables = [
            MockTable("table1", False),
            MockTable("table2", False),
            MockTable("temp_table", True)  # Should be skipped
        ]
        mock_spark.catalog.listTables = MagicMock(return_value=mock_tables)
        
        # Mock table reads
        mock_df = MagicMock()
        from pyspark.sql.types import StructType, StructField, IntegerType
        mock_df.schema = StructType([StructField("id", IntegerType(), True)])
        mock_df.createOrReplaceTempView = MagicMock()
        mock_spark.read.table = MagicMock(return_value=mock_df)
        
        # Mock EXPLAIN result
        explain_row = MagicMock()
        explain_row.__getitem__ = MagicMock(return_value="FileScan delta")
        explain_df = MagicMock()
        explain_df.take = MagicMock(return_value=[explain_row])
        mock_spark.sql = MagicMock(return_value=explain_df)
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        result = orchestrator.run_smart_scan()
        
        assert result["status"] == "FINISHED"
        assert result["scanned_objects"] == 2  # Only non-temp tables
    
    def test_smart_scan_no_objects(self):
        """Test smart scan when no objects found."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(return_value="true")
        mock_spark.catalog.currentDatabase = MagicMock(return_value="empty_db")
        mock_spark.catalog.listTables = MagicMock(return_value=[])
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        result = orchestrator.run_smart_scan()
        
        assert result["status"] == "SKIPPED"
        assert result["scanned"] == 0
    
    def test_error_handling_invalid_table(self):
        """Test error handling when table doesn't exist."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(return_value="true")
        mock_spark.read.table = MagicMock(side_effect=Exception("Table not found"))
        
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        
        # Should handle error gracefully
        result = orchestrator.run_smart_scan(target_table="nonexistent_table")
        
        assert result["status"] == "FINISHED"
        assert result["scanned_objects"] == 0


@pytest.mark.system
class TestFullWorkflowWithMockData:
    """System tests simulating full workflow with realistic mock data."""
    
    def test_complete_audit_workflow_with_issues(self, capsys):
        """Test complete audit workflow detecting multiple issues."""
        mock_spark = MagicMock()
        mock_spark.conf.get = MagicMock(return_value="true")
        mock_spark.catalog.currentDatabase = MagicMock(return_value="bronze")
        
        # Create mock DataFrame with problematic schema
        mock_df = MagicMock()
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        mock_df.schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("temperature", StringType(), True),  # Problem: should be numeric
            StructField("voltage", StringType(), True),      # Problem: should be numeric
            StructField("bad_partition_key", StringType(), True)  # Problem: high cardinality
        ])
        mock_df.createOrReplaceTempView = MagicMock()
        
        # Mock EXPLAIN with multiple issues
        explain_plan = """
        == Physical Plan ==
        FileScan delta [temperature#123:string, voltage#456:string]
        PartitionColumns: [bad_partition_key, minute]
        SortMergeJoin [id#789]
        Exchange hashpartitioning
        """
        
        explain_row = MagicMock()
        explain_row.__getitem__ = MagicMock(return_value=explain_plan)
        explain_df = MagicMock()
        explain_df.take = MagicMock(return_value=[explain_row])
        mock_spark.sql = MagicMock(return_value=explain_df)
        
        # Mock file listing showing many small files
        class MockFileInfo:
            def __init__(self, path, size):
                self.path = path
                self.size = size
        
        with patch('src.readers.dataframe_reader.DBUtils') as mock_dbutils_class:
            mock_dbutils = MagicMock()
            mock_dbutils.fs.ls = MagicMock(return_value=[
                MockFileInfo(f"/path/file{i}.parquet", 1024) for i in range(200)
            ])
            mock_dbutils_class.return_value = mock_dbutils
            
            # Run complete workflow
            orchestrator = APMAutomatedOrchestrator(mock_spark)
            result = orchestrator.run_smart_scan(df=mock_df)
            
            # Verify results
            assert result["status"] == "FINISHED"
            
            # Check output contains multiple alerts
            captured = capsys.readouterr()
            
            # Should detect: small files, type casting, over-partitioning, missed broadcast
            assert "PERF-001" in captured.out  # Small files
            assert "PERF-002" in captured.out  # Missed broadcast
            assert "PERF-003" in captured.out  # Type casting
            assert "PERF-004" in captured.out  # Over-partitioning
            
            # Should show recommendations
            assert "Recommendation:" in captured.out
            assert "Fix Template:" in captured.out
            
            # Should show cost estimate
            assert "Estimated FinOps waste:" in captured.out
            assert "$" in captured.out
