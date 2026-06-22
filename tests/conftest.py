# CRITICAL: Disable bytecode caching FIRST for Databricks Workspace compatibility
import sys
sys.dont_write_bytecode = True

import pytest
import os
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime

# Add project root to path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.auditor.models import Alert, ClusterContext, Suggestion, AuditReport

@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing."""
    # For unit tests, we'll use mock spark
    # For integration/system tests, this would be a real Spark session
    return None

@pytest.fixture
def mock_spark():
    """Mock SparkSession for unit tests."""
    spark = MagicMock(spec=SparkSession)
    spark.conf.get = MagicMock(return_value="true")
    spark.catalog.currentCatalog = MagicMock(return_value="spark_catalog")
    spark.catalog.currentDatabase = MagicMock(return_value="default")
    spark.catalog.listTables = MagicMock(return_value=[])
    spark.catalog.tableExists = MagicMock(return_value=True)
    return spark

@pytest.fixture
def sample_alert():
    """Sample Alert object for testing."""
    return Alert(
        rule_id="PERF-001",
        title="Small files problem detected",
        description="Table contains 200 files, exceeding recommended threshold of 100.",
        fix="Run OPTIMIZE operation on the table.",
        severity="WARNING",
        context="test_context"
    )

@pytest.fixture
def sample_cluster_context():
    """Sample ClusterContext for testing."""
    return ClusterContext(
        is_serverless=True,
        aqe_enabled=True,
        photon_enabled=True
    )

@pytest.fixture
def sample_suggestion():
    """Sample Suggestion for testing."""
    return Suggestion(
        rule_id="PERF-001",
        remediation_text="Run data compaction operation.",
        code_template='spark.sql("OPTIMIZE {table_name}")'
    )

@pytest.fixture
def sample_audit_report(sample_cluster_context):
    """Sample AuditReport for testing."""
    return AuditReport(
        context_name="test_audit",
        timestamp=datetime.now(),
        cluster_context=sample_cluster_context,
        alerts=[],
        suggestions=[],
        total_estimated_waste_usd=0.0
    )

@pytest.fixture
def sample_explain_plan():
    """Sample Catalyst execution plan text."""
    return """
    == Physical Plan ==
    FileScan delta [id#123, name#456] 
    PartitionFilters: []
    DataFilters: []
    Exchange hashpartitioning(id#123, 200)
    +- SortMergeJoin [id#123], [station_id#789]
       :- Sort [id#123 ASC NULLS FIRST]
       +- Exchange hashpartitioning(id#123, 200)
    """

@pytest.fixture
def sample_metrics():
    """Sample physical metrics dictionary."""
    return {
        "num_files": 200,
        "total_size_bytes": 1048576,
        "schema_fields": {
            "id": "int",
            "temperature": "string",
            "voltage": "string",
            "timestamp": "string"
        }
    }

@pytest.fixture
def sample_policies():
    """Sample policy configuration."""
    return {
        "small_files": {"max_file_count": 100, "threshold_size_mb": 10.0},
        "data_skew": {"max_duration_ratio": 5.0},
        "over_partitioning": {"max_partitions": 1000},
        "finops": {"dbu_cost_per_hour": 0.40, "estimated_core_count": 8}
    }

@pytest.fixture
def mock_dataframe():
    """Mock DataFrame for testing."""
    df = MagicMock()
    df.schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("temperature", StringType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])
    df.createOrReplaceTempView = MagicMock()
    return df

@pytest.fixture
def mock_dbutils():
    """Mock DBUtils for testing."""
    dbutils = MagicMock()
    
    # Mock file listing
    class MockFileInfo:
        def __init__(self, path, size):
            self.path = path
            self.size = size
    
    dbutils.fs.ls = MagicMock(return_value=[
        MockFileInfo("/path/file1.parquet", 1024),
        MockFileInfo("/path/file2.parquet", 2048),
        MockFileInfo("/path/_delta_log/", 512)  # Should be filtered out
    ])
    
    return dbutils
