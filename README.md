# 🚀 APM Spark Auditor - Automated Performance Monitoring & FinOps Engine

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![PySpark 4.0.3](https://img.shields.io/badge/pyspark-4.0.3-orange.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Enterprise-grade automated performance auditor for Apache Spark and Databricks workloads**

APM Spark Auditor is an intelligent performance monitoring framework that automatically detects anti-patterns in Spark execution plans, provides actionable remediation suggestions, and estimates the financial impact of performance issues. It works seamlessly with both traditional Spark clusters and modern Databricks Serverless/Connect architectures.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Key Features](#-key-features)
- [Detected Performance Issues](#-detected-performance-issues)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Usage Examples](#-usage-examples)
- [Configuration](#-configuration)
- [Testing](#-testing)
- [Project Structure](#-project-structure)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🎯 Overview

The APM Spark Auditor analyzes Spark execution plans (Catalyst optimizer output) and DataFrame schemas to identify performance bottlenecks, inefficient query patterns, and cost optimization opportunities. It provides:

- **Automated Detection**: 10 comprehensive rules covering the most common Spark performance anti-patterns
- **FinOps Integration**: Financial impact estimation for identified issues (DBU cost calculation)
- **Actionable Remediation**: Code-level suggestions and templates for fixing detected problems
- **Multi-Environment Support**: Works with vanilla Spark, Databricks clusters, and Serverless/Connect
- **Zero-Overhead Analysis**: Non-intrusive inspection of execution plans without job execution

---

## ✨ Key Features

### 🔍 **Intelligent Analysis Engine**
- Parses Catalyst physical execution plans
- Analyzes DataFrame schemas and metadata
- Detects structural and query-level anti-patterns
- Context-aware recommendations based on cluster type

### 💰 **FinOps Cost Estimation**
- Calculates estimated waste in USD
- Factors in DBU pricing and cluster configuration
- Provides monthly cost projections for persistent issues
- Supports both real Databricks costs and simulation mode

### 🛠️ **Comprehensive Remediation**
- Generates targeted suggestions for each detected issue
- Includes executable code templates
- Adapts recommendations to cluster context (Serverless vs Classic)

### 🎭 **Multi-Platform Compatibility**
- **Traditional Spark**: Local and cluster deployments
- **Databricks Classic**: Single-user and shared clusters
- **Databricks Serverless**: Spark Connect architecture
- **Docker**: Containerized execution support

### 📊 **Rich Reporting**
- Console output with detailed alerts
- Severity levels (CRITICAL, HIGH, WARNING)
- Metrics capture for audit trails
- Structured data models for integration

---

## 🐛 Detected Performance Issues

The engine detects **10 critical performance anti-patterns** (PERF-001 to PERF-010):

| Rule ID | Issue | Impact | Severity |
|---------|-------|--------|----------|
| **PERF-001** | **Small Files Problem** | Excessive metadata management, slow reads | WARNING |
| **PERF-002** | **Missed Broadcast Join** | Unnecessary shuffle operations on small tables | WARNING |
| **PERF-003** | **Type Casting in Filters** | Blocks Data Skipping indexes, forces full table scans | WARNING |
| **PERF-004** | **Over-Partitioning** | Directory explosion, Driver JVM paralysis | HIGH |
| **PERF-005** | **Cartesian Product** | Memory explosion, OutOfMemory risk | CRITICAL |
| **PERF-006** | **Data Skew** | Unbalanced workload distribution | HIGH |
| **PERF-007** | **Missing Physical Optimization** | No Z-Order/Liquid Clustering indexes | WARNING |
| **PERF-008** | **Explode Abuse** | Avalanche row multiplication in memory | WARNING |
| **PERF-009** | **Redundant Table Scan** | Multiple reads of same source without caching | WARNING |
| **PERF-010** | **Non-Vectorized UDF** | Row-by-row processing, JVM/Python serialization overhead | HIGH |

---

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    APMAutomatedOrchestrator                      │
│                  (Main Entry Point & Coordinator)                │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
┌───────────────┐    ┌──────────────┐    ┌─────────────────┐
│ Readers       │    │ Auditor      │    │ Reporters       │
│ - DataFrame   │───▶│ - Engine     │───▶│ - Console       │
│ - EventLog    │    │ - Models     │    │ - (Extensible)  │
└───────────────┘    └──────┬───────┘    └─────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌──────────────┐   ┌────────────────┐
│ Rules         │   │ Suggestions  │   │ FinOps         │
│ - Physical    │   │ - Templates  │   │ - Cost Calc    │
│ - Query       │   │ - Remediation│   │ - Waste Est.   │
└───────────────┘   └──────────────┘   └────────────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            ▼
                   ┌────────────────┐
                   │ Context        │
                   │ - Env Provider │
                   │ - Policies     │
                   └────────────────┘
```

### Core Modules

#### 1. **Auditor** (`src/auditor/`)
- [`engine.py`](src/auditor/engine.py) - Main orchestration engine coordinating all components
- [`models.py`](src/auditor/models.py) - Data models (Alert, ClusterContext, AuditReport, Suggestion)

#### 2. **Rules** (`src/rules/`)
- [`physical_rules.py`](src/rules/physical_rules.py) - Infrastructure-level detections (files, partitioning, indexing)
- [`query_rules.py`](src/rules/query_rules.py) - Query-level detections (joins, UDFs, type casting)

#### 3. **Readers** (`src/readers/`)
- [`dataframe_reader.py`](src/readers/dataframe_reader.py) - Extracts plans and metrics from DataFrames
- [`event_log_reader.py`](src/readers/event_log_reader.py) - Parses Spark event logs for runtime metrics

#### 4. **Suggestions** (`src/suggestions/`)
- [`remediation_engine.py`](src/suggestions/remediation_engine.py) - Generates context-aware recommendations
- [`suggestions_templates.py`](src/suggestions/suggestions_templates.py) - Predefined remediation code templates

#### 5. **FinOps** (`src/finops/`)
- [`cost_translator.py`](src/finops/cost_translator.py) - Converts performance issues to financial estimates

#### 6. **Context** (`src/context/`)
- [`environment_provider.py`](src/context/environment_provider.py) - Detects cluster type (Serverless, AQE, Photon)

#### 7. **Policies** (`src/policies/`)
- [`policy_manager.py`](src/policies/policy_manager.py) - Manages detection thresholds and FinOps parameters

#### 8. **Reporters** (`src/reporters/`)
- [`console_reporter.py`](src/reporters/console_reporter.py) - Formats and displays audit results

---

## 📦 Installation

### Prerequisites

- **Python 3.11** (strictly enforced for environment parity)
- **PySpark 4.0.3+**
- **Databricks Runtime 13.x+** (for cloud deployments)

### Method 1: Local Development (Editable Install)

```bash
# Clone the repository
git clone https://github.com/your-org/databricks-engine-optimization.git
cd databricks-engine-optimization

# Create virtual environment with Python 3.11
python3.11 -m venv venv
source venv/bin/activate

# Install in editable mode with all dependencies
pip install -e .[dev,test]
```

### Method 2: Via Wheel Distribution

```bash
# Build the wheel package
make package

# Install the wheel
pip install dist/apm_spark_auditor-1.0.0-py3-none-any.whl
```

### Method 3: Docker Environment

```bash
# Build the Docker image
make docker-build

# Run the container
make docker-up
```

### Method 4: Databricks Deployment

```bash
# 1. Build the wheel package
make package

# 2. Upload to Databricks workspace
databricks fs cp dist/apm_spark_auditor-1.0.0-py3-none-any.whl dbfs:/FileStore/libs/

# 3. Install in notebook
%pip install /dbfs/FileStore/libs/apm_spark_auditor-1.0.0-py3-none-any.whl
```

---

## 🚀 Quick Start

### Basic Usage - Analyze a DataFrame

```python
from pyspark.sql import SparkSession
from apm_spark_auditor.run.main_trigger import APMAutomatedOrchestrator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("APM-Audit-Demo") \
    .getOrCreate()

# Load your data
df = spark.read.table("my_catalog.my_schema.my_table")

# Run the audit
orchestrator = APMAutomatedOrchestrator(spark)
result = orchestrator.run_smart_scan(df=df, custom_context="my_etl_pipeline")

print(f"Scan completed: {result}")
```

### Command-Line Interface

```bash
# Install the package to enable CLI command
pip install -e .

# Run audit on a specific table
apm-audit --table "catalog.schema.table_name"

# Run with custom context
apm-audit --table "catalog.schema.table_name" --context "production_etl"
```

---

## 💡 Usage Examples

### Example 1: Detecting Small Files Problem

```python
from pyspark.sql import SparkSession
from apm_spark_auditor.run.main_trigger import APMAutomatedOrchestrator

spark = SparkSession.builder.master("local[*]").appName("Small-Files-Demo").getOrCreate()

# Create a DataFrame with fragmented writes (anti-pattern)
df = spark.range(1000).repartition(500)  # 500 tiny partitions!
df.write.mode("overwrite").format("delta").save("/tmp/fragmented_table")

# Audit the result
df_read = spark.read.format("delta").load("/tmp/fragmented_table")
orchestrator = APMAutomatedOrchestrator(spark)
orchestrator.run_smart_scan(df=df_read, custom_context="fragmented_demo")
```

**Expected Output:**
```
[1] Alert ID: PERF-001 (WARNING)
    Title: Small files problem detected
    Description: Table contains 500 files, exceeding recommended threshold of 100...
    💡 Recommendation: Run data compaction operation (OPTIMIZE) on Delta Lake table
    💻 Fix Template:
    spark.sql("OPTIMIZE {table_name}")
```

### Example 2: Multi-Anomaly Detection (Demo Script)

The included [`demo.py`](demo.py) demonstrates multiple anti-patterns:

```python
python demo.py
```

This script intentionally creates:
- String-typed numeric metrics (PERF-003)
- Non-vectorized Python UDF (PERF-010)
- Explode abuse (PERF-008)
- Redundant scans without caching (PERF-009)
- Cartesian product (PERF-005)

### Example 3: FinOps Cost Simulation

```python
orchestrator = APMAutomatedOrchestrator(spark)

# Enable cost simulation for local testing
result = orchestrator.run_smart_scan(
    df=my_dataframe,
    custom_context="production_pipeline",
    simulate_cloud_costs=True  # Estimates monthly waste
)
```

### Example 4: Batch Audit Multiple Tables

```python
tables = [
    "catalog.bronze.bess_telemetry",
    "catalog.bronze.pv_metrics",
    "catalog.silver.enriched_data"
]

orchestrator = APMAutomatedOrchestrator(spark)

for table_name in tables:
    print(f"\n{'='*60}")
    print(f"Auditing: {table_name}")
    print('='*60)
    orchestrator.run_smart_scan(target_table=table_name)
```

---

## ⚙️ Configuration

### Policy Configuration

Customize detection thresholds via [`PolicyManager`](src/policies/policy_manager.py):

```python
from apm_spark_auditor.policies.policy_manager import PolicyManager

custom_policies = {
    "small_files": {
        "max_file_count": 200,        # Alert when exceeding 200 files
        "threshold_size_mb": 5.0       # Minimum file size threshold
    },
    "data_skew": {
        "max_duration_ratio": 10.0     # Max acceptable task duration variance
    },
    "over_partitioning": {
        "max_partitions": 500           # Maximum safe partition count
    },
    "finops": {
        "dbu_cost_per_hour": 0.55,     # DBU pricing (USD)
        "estimated_core_count": 16      # Cluster core count
    }
}

policy_manager = PolicyManager(custom_policies)
```

### Environment Detection

The system automatically detects runtime context:

- **Databricks Serverless**: Spark Connect architecture detection
- **Databricks Classic**: Cluster ID and configuration inspection
- **Vanilla Spark**: Local or standalone cluster mode

Override detection if needed:

```python
from apm_spark_auditor.context.environment_provider import EnvironmentProvider

env_provider = EnvironmentProvider(spark)
cluster_ctx = env_provider.determine_cluster_context()

print(f"Is Serverless: {cluster_ctx.is_serverless}")
print(f"AQE Enabled: {cluster_ctx.aqe_enabled}")
print(f"Photon Enabled: {cluster_ctx.photon_enabled}")
```

---

## 🧪 Testing

The project includes comprehensive test coverage with environment-aware routing.

### Run All Tests (Local Environment)

```bash
# Using make
make test

# Using pytest directly
pytest tests/ --env=local
```

### Run Databricks-Specific Tests

```bash
pytest tests/ --env=databricks
```

### Test Categories

Tests are organized by markers:

- `@pytest.mark.unit` - Fast, isolated unit tests with mocks
- `@pytest.mark.integration` - Component integration tests
- `@pytest.mark.system` - End-to-end scenario tests
- `@pytest.mark.local_spark` - Local Spark environment only
- `@pytest.mark.databricks` - Databricks runtime required

### Run Specific Test Category

```bash
# Unit tests only
pytest tests/ -m unit

# Integration tests
pytest tests/ -m integration

# Databricks tests (requires Databricks environment)
pytest tests/ -m databricks --env=databricks
```

### Test Structure

```
tests/
├── conftest.py                          # Shared fixtures and environment routing
├── unit/                                # Fast unit tests with mocks
│   ├── test_engine_local.py
│   ├── test_engine_databricks.py
│   ├── test_rules_local.py
│   └── test_rules_databricks.py
├── integration/                         # Component integration tests
│   └── test_engine_integration_local.py
└── system/                              # End-to-end tests
    ├── test_e2e_local.py
    └── test_e2e_databricks.py
```

### Coverage Report

```bash
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html
```

---

## 📁 Project Structure

```
databricks-engine-optimization/
├── src/                                 # Main source code
│   ├── adapters/                        # Execution plan readers
│   │   ├── base.py                      # Abstract interfaces
│   │   └── vanilla_spark.py             # Traditional Spark adapter
│   ├── auditor/                         # Core auditing engine
│   │   ├── engine.py                    # Main orchestration engine
│   │   └── models.py                    # Data models (Alert, Report, etc.)
│   ├── context/                         # Environment detection
│   │   └── environment_provider.py      # Cluster context analyzer
│   ├── decorators/                      # Utility decorators
│   │   └── __init__.py                  # Tracing and error handling
│   ├── finops/                          # Cost calculation
│   │   └── cost_translator.py           # Financial impact estimator
│   ├── policies/                        # Configuration management
│   │   └── policy_manager.py            # Threshold and policy manager
│   ├── readers/                         # Metrics extraction
│   │   ├── dataframe_reader.py          # DataFrame plan/metrics reader
│   │   └── event_log_reader.py          # Event log parser
│   ├── reporters/                       # Output formatting
│   │   └── console_reporter.py          # Console output formatter
│   ├── rules/                           # Detection rules
│   │   ├── physical_rules.py            # Infrastructure-level rules
│   │   └── query_rules.py               # Query-level rules
│   ├── suggestions/                     # Remediation engine
│   │   ├── remediation_engine.py        # Suggestion generator
│   │   └── suggestions_templates.py     # Code templates
│   └── run/                             # Entry points
│       └── main_trigger.py              # Main orchestrator & CLI
├── tests/                               # Test suite
│   ├── conftest.py                      # Pytest configuration & fixtures
│   ├── unit/                            # Unit tests
│   ├── integration/                     # Integration tests
│   └── system/                          # End-to-end tests
├── generators/                          # Test data generators
│   ├── data/
│   │   └── mock_raw_generator.py        # Mock telemetry data
│   └── etl/
│       └── bronze_pipelines.py          # ETL anti-pattern generators
├── notebooks/                           # Databricks notebooks
│   ├── 01_laboratory_setup.ipynb        # Environment setup
│   ├── 02_performance_auditor.ipynb     # Audit demonstrations
│   └── trigger.ipynb                    # Quick run notebook
├── docker/                              # Docker configuration
│   └── app/
│       └── dockerfile                   # Container definition
├── demo.py                              # Standalone demo script
├── docker-compose.yaml                  # Docker orchestration
├── Makefile                             # Development automation
├── pyproject.toml                       # Project metadata & dependencies
├── requirements.txt                     # Dependency lock file
├── README.md                            # This file
└── .gitignore                           # Git exclusions
```

---

## 🔧 Development

### Make Commands

The project includes a [`Makefile`](Makefile) for common tasks:

```bash
make help          # Display all available commands
make venv          # Create Python 3.11 virtual environment
make test          # Run unit tests with coverage
make package       # Build .whl distribution artifact
make docker-build  # Build Docker image
make docker-up     # Start Docker container
make docker-down   # Stop Docker container
make clean         # Remove build artifacts
make clean-all     # Remove build artifacts and venv
```

### Code Quality

The project uses:
- **Black** for code formatting (line length: 100)
- **Ruff** for linting (Python 3.11 target)
- **Pytest** for testing

```bash
# Format code
black src/ tests/ --line-length 100

# Lint code
ruff check src/ tests/

# Run tests with coverage
pytest tests/ --cov=src --cov-report=term-missing
```

### Adding New Rules

To add a new detection rule:

1. Create rule class in [`src/rules/physical_rules.py`](src/rules/physical_rules.py) or [`src/rules/query_rules.py`](src/rules/query_rules.py):

```python
class MyNewRule(IAnalysisRule):
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], 
                 policies: Dict[str, Any] = None) -> Optional[Alert]:
        # Detection logic here
        if condition_detected:
            return Alert(
                rule_id="PERF-011",
                title="My New Issue",
                description="Detailed description...",
                fix="How to fix this...",
                severity="WARNING"
            )
        return None
```

2. Add suggestion template in [`src/suggestions/suggestions_templates.py`](src/suggestions/suggestions_templates.py):

```python
TEMPLATES["PERF-011"] = {
    "text": "Do this to fix the issue...",
    "code": """# Example code
df.optimized_operation()"""
}
```

3. Register rule in [`src/run/main_trigger.py`](src/run/main_trigger.py):

```python
self.active_rules = [
    # ... existing rules ...
    MyNewRule()  # Add your new rule
]
```

---

## 📊 Output Example

```
============================================================
📊 [APM CORE REPORT] Context: enterprise_bess_heavy_telemetry_pipeline
Timestamp: 2026-06-29T09:20:15.123456
Estimated FinOps waste: $124.85 / month
============================================================

[1] Alert ID: PERF-003 (WARNING)
    Title: Improper data typing (Type Casting & Metric Degradation)
    Description: Critical typing anomalies detected: metric 'temperature' stored as STRING...
    💡 Recommendation: Remove explicit type casting in filter conditions
    💻 Fix Template:
    # Instead of:
    df.filter(F.col("temperature").cast("string") == "25")
    # Use correct data type:
    df.filter(F.col("temperature") == 25)

[2] Alert ID: PERF-010 (HIGH)
    Title: Unverified UDF function usage (Row-by-Row Execution)
    Description: Standard Python UDF function detected. This disables processor vectorization...
    💡 Recommendation: Replace custom Python code with built-in functions
    💻 Fix Template:
    # Replace Python UDF with built-in functions from pyspark.sql.functions

[3] Alert ID: PERF-008 (WARNING)
    Title: Inefficient structure explosion (Explode Abuse)
    Description: Use of explode() function forces physical row duplication...
    💡 Recommendation: Replace explode() with vectorized higher-order functions
    💻 Fix Template:
    # Replace explode() with transform(), filter(), or aggregate()

[4] Alert ID: PERF-009 (WARNING)
    Title: Redundant Source Scanning (Redundant Table Scan)
    Description: Detected that same Delta Lake data source is read 3 times...
    💡 Recommendation: Apply caching before logic branching
    💻 Fix Template:
    df.cache() or df.persist()

[5] Alert ID: PERF-005 (CRITICAL)
    Title: Cartesian product detected
    Description: Catalyst engine was forced to perform join without condition...
    💡 Recommendation: Check join conditions or use broadcast() for intentional CROSS JOIN
    💻 Fix Template:
    # Ensure proper join condition or use broadcast for small table

============================================================
```

---

## 🌟 Use Cases

### 1. **Development-Time Quality Gates**
Integrate into CI/CD pipelines to catch performance issues before production deployment.

### 2. **Production Monitoring**
Run periodic audits on production tables to identify optimization opportunities.

### 3. **Cost Optimization**
Identify high-cost anti-patterns and estimate savings from remediation.

### 4. **Training & Education**
Use the detailed explanations and code templates to educate teams on Spark best practices.

### 5. **Migration Assessment**
Audit legacy Spark jobs before migrating to Databricks or upgrading Spark versions.

---

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Follow code style (Black formatting, type hints)
4. Add tests for new functionality
5. Ensure all tests pass (`make test`)
6. Commit with clear messages (`git commit -m 'Add amazing feature'`)
7. Push to your branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 👤 Author

**Kacper Grodecki**
- Email: kacpra.grodeckiego@gmail.com

---

## 🙏 Acknowledgments

- Apache Spark community for the powerful distributed computing framework
- Databricks for innovation in lakehouse architecture and Photon engine
- All contributors to the PySpark ecosystem

---

## 📚 Additional Resources

- [Apache Spark Performance Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)
- [Databricks Best Practices](https://docs.databricks.com/optimizations/index.html)
- [Delta Lake Optimization](https://docs.delta.io/latest/optimizations-oss.html)
- [Catalyst Optimizer Deep Dive](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)

---

## 🐛 Known Issues & Limitations

1. **Event Log Reader**: Requires Spark history server or event log directory access
2. **Databricks Connect**: Some JVM-level metrics may be unavailable in Serverless mode
3. **Cost Estimation**: FinOps calculations are estimates based on configurable DBU pricing
4. **EXPLAIN Plan Parsing**: Custom Catalyst optimizations may not be fully recognized

---

## 🗺️ Roadmap

- [ ] Integration with Databricks Jobs API for automated scanning
- [ ] REST API for remote auditing
- [ ] Support for Spark Structured Streaming query analysis  
- [ ] Grafana/Prometheus metrics export
- [ ] Machine learning-based anomaly detection
- [ ] Integration with Git hooks for pre-commit checks
- [ ] Web UI dashboard for historical audit tracking

---

**Made with ❤️ for the Spark performance community**
