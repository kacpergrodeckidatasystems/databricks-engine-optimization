# tests/conftest.py
import sys
import os
import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime

# CRITICAL: Disable bytecode caching FIRST for Databricks Workspace compatibility
sys.dont_write_bytecode = True

# Dynamic project root mapping for clean absolute imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.auditor.models import Alert, ClusterContext, Suggestion, AuditReport
from src.context.environment_provider import EnvironmentProvider


# =========================================================================
# ⚙️ PYTEST HOOKS & ADAPTIVE ORCHESTRATION (Files + Markers)
# =========================================================================
def pytest_addoption(parser):
    """Register the --env command-line option in the pytest CLI configuration."""
    parser.addoption(
        "--env",
        action="store",
        default="local",
        choices=["local", "databricks"],
        help="Select runtime target environment: 'local' or 'databricks'",
    )


def pytest_runtest_setup(item):
    """
    Comprehensive test routing hook.
    Automatically filters and skips test execution based on filename suffixes
    and explicit environment decorators/markers.
    """
    env_option = item.config.getoption("--env")
    file_name = os.path.basename(item.fspath)

    # 1. Route based on file name patterns (*_local.py / *_databricks.py)
    if "_databricks" in file_name and env_option != "databricks":
        pytest.skip(f"🔒 Skipped {file_name}: Test module requires Databricks environment")

    if "_local" in file_name and env_option != "local":
        pytest.skip(f"🔒 Skipped {file_name}: Test module is dedicated to local environment")

    # 2. Route based on explicit decorator markers (for shared files like integration tests)
    if "databricks" in item.keywords and env_option != "databricks":
        pytest.skip("🔒 Skipped test: Requires Databricks runtime context (--env=databricks)")

    if "local_spark" in item.keywords and env_option != "local":
        pytest.skip("🔒 Skipped test: Dedicated to vanilla local Spark context (--env=local)")


# =========================================================================
# 🌍 ENVIRONMENT-AWARE FIXTURES
# =========================================================================
@pytest.fixture
def environment_type(request):
    """Returns the active environment configuration string passed via CLI option."""
    return request.config.getoption("--env")


@pytest.fixture
def is_databricks(environment_type):
    """Returns a boolean flag indicating if the target context is Databricks."""
    return environment_type == "databricks"


@pytest.fixture(scope="session")
def spark_session(request):
    """
    Provides a stable SparkSession instances tailored specifically to the pipeline.
    Simulates Hive metastore for Databricks or uses a lightweight configuration for local testing.
    """
    env = request.config.getoption("--env")
    if env == "databricks":
        os.environ["DATABRICKS_RUNTIME_VERSION"] = "17.x-DBR"
        spark = (
            SparkSession.builder.appName("APM-Auditor-Databricks-Simulation")
            .master("local[*]")
            .config("spark.sql.catalogImplementation", "hive")
            .getOrCreate()
        )
        yield spark
        spark.stop()
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            del os.environ["DATABRICKS_RUNTIME_VERSION"]
    else:
        spark = (
            SparkSession.builder.appName("APM-Auditor-Vanilla-Local-Test")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        yield spark
        spark.stop()


@pytest.fixture
def mock_spark():
    """Returns a highly responsive, standalone mock SparkSession for fast unit tests."""
    spark = MagicMock(spec=SparkSession)
    spark.conf.get = MagicMock(return_value="true")
    spark.catalog.currentCatalog = MagicMock(return_value="spark_catalog")
    spark.catalog.currentDatabase = MagicMock(return_value="default")
    spark.catalog.listTables = MagicMock(return_value=[])
    spark.catalog.tableExists = MagicMock(return_value=True)
    return spark


@pytest.fixture
def spark_or_mock(environment_type, mock_spark, spark_session):
    """Adaptive fixture injecting either a live functional SparkSession or a fast isolation mock."""
    if environment_type == "databricks":
        return spark_session
    return mock_spark


@pytest.fixture
def mock_dbutils():
    """Provides a safe DBUtils file system mock for testing paths without storage dependencies."""
    dbutils = MagicMock()

    class MockFileInfo:
        def __init__(self, path, size):
            self.path = path
            self.size = size

    dbutils.fs.ls = MagicMock(
        return_value=[
            MockFileInfo("/path/file1.parquet", 1024),
            MockFileInfo("/path/file2.parquet", 2048),
            MockFileInfo("/path/_delta_log/", 512),
        ]
    )
    return dbutils


@pytest.fixture
def dbutils_or_mock(environment_type, mock_dbutils):
    """Dynamic gateway delivering authentic cloud DBUtils or a secure mock abstraction layer."""
    if environment_type == "databricks":
        try:
            from pyspark.dbutils import DBUtils

            spark = SparkSession.builder.getOrCreate()
            return DBUtils(spark)
        except:
            return mock_dbutils
    return mock_dbutils


@pytest.fixture
def environment_provider(spark_or_mock):
    """Returns a configured EnvironmentProvider instance wrapping the appropriate execution engine."""
    return EnvironmentProvider(spark_or_mock)


# =========================================================================
# 📊 MODEL DATA FIXTURES & SAMPLES
# =========================================================================
@pytest.fixture
def sample_policies():
    """Returns a dictionary containing predefined system SLA thresholds and FinOps boundary configs."""
    return {
        "small_files": {"max_file_count": 100, "threshold_size_mb": 10.0},
        "data_skew": {"max_duration_ratio": 5.0},
        "over_partitioning": {"max_partitions": 1000},
        "finops": {"dbu_cost_per_hour": 0.40, "estimated_core_count": 8},
    }


@pytest.fixture
def sample_alert():
    """Returns a baseline mock Alert Pydantic model for verification schemas."""
    return Alert(
        rule_id="PERF-001",
        title="Small files problem detected",
        description="Table contains 200 files, exceeding recommended threshold of 100.",
        fix="Run OPTIMIZE operation on the table.",
        severity="WARNING",
        context="test_context",
    )


@pytest.fixture
def sample_cluster_context():
    """Provides a baseline ClusterContext telemetry entity configuration."""
    return ClusterContext(is_serverless=True, aqe_enabled=True, photon_enabled=True)


@pytest.fixture
def sample_suggestion():
    """Returns a populated Suggestion structure instance mapping standard remediations."""
    return Suggestion(
        rule_id="PERF-001",
        remediation_text="Run data compaction operation.",
        code_template='spark.sql("OPTIMIZE {table_name}")',
    )


@pytest.fixture
def sample_audit_report(sample_cluster_context):
    """Provides an initialized AuditReport container instance."""
    return AuditReport(
        context_name="test_audit",
        timestamp=datetime.now(),
        cluster_context=sample_cluster_context,
        alerts=[],
        suggestions=[],
        total_estimated_waste_usd=0.0,
    )


@pytest.fixture
def sample_explain_plan():
    """Returns a typical raw Catalyst execution text plan containing performance anti-patterns."""
    return """
    == Physical Plan ==
    FileScan delta [id#123, name#456] 
    PartitionFilters: []
    DataFilters: []
    Exchange hashpartitioning(id#123, 200)
    +- SortMergeJoin [id#123], [station_id#789]
    """


@pytest.fixture
def sample_metrics():
    """Provides a data dictionary representing file metrics and layout configurations."""
    return {
        "num_files": 200,
        "total_size_bytes": 1048576,
        "schema_fields": {"id": "int", "temperature": "string"},
    }


@pytest.fixture
def mock_dataframe():
    """Returns a mocked PySpark DataFrame structure with static telemetry schema configurations."""
    df = MagicMock()
    df.schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("temperature", StringType(), True),
            StructField("voltage", DoubleType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )
    df.createOrReplaceTempView = MagicMock()
    return df
