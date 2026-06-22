# APM Engine Optimization - Test Suite

Comprehensive test suite for the Databricks Engine Optimization framework covering unit tests, integration tests, and system-level end-to-end tests.

## Test Structure

```
tests/
├── conftest.py              # Shared pytest fixtures
├── pytest.ini               # Pytest configuration
├── requirements.txt         # Test dependencies
├── README.md               # This file
│
├── unit/                   # Unit tests for individual components
│   ├── test_physical_rules.py
│   ├── test_query_rules.py
│   ├── test_engine.py
│   └── ...
│
├── integration/            # Integration tests for component interactions
│   ├── test_engine_integration.py
│   └── ...
│
└── system/                 # System-level end-to-end tests
    ├── test_end_to_end.py
    └── ...
```

## Test Categories

### Unit Tests (`tests/unit/`)

Test individual components in isolation:

- **test_physical_rules.py**: Tests for physical plan analysis rules
  - SmallFilesRule
  - MissedBroadcastRule
  - TypeCastingRule
  - OverPartitioningRule
  - DataSkewRule
  - MissingOptimizationRule

- **test_query_rules.py**: Tests for query pattern analysis rules
  - CartesianProductRule
  - ExplodeAbuseRule
  - RedundantScanRule
  - NonVectorizedUdfRule

- **test_engine.py**: Tests for core engine components
  - PerformanceEngine
  - PolicyManager
  - CostTranslator
  - RemediationEngine

### Integration Tests (`tests/integration/`)

Test component interactions:

- **test_engine_integration.py**: Tests for engine working with readers and multiple rules
  - Engine + DataFrameExplainReader integration
  - Multiple rules triggering simultaneously
  - Cost calculation across components

### System Tests (`tests/system/`)

End-to-end workflow tests:

- **test_end_to_end.py**: Full workflow tests
  - APM Orchestrator initialization
  - Platform detection (Databricks vs Vanilla Spark)
  - Context discovery
  - Smart scan with different inputs (DataFrame, table name, auto-discovery)
  - Complete audit workflow with multiple issues
  - Error handling

## Running Tests

### Prerequisites

Install test dependencies:

```bash
pip install -r tests/requirements.txt
```

### Run All Tests

```bash
# From project root
pytest tests/

# Or from tests directory
cd tests
pytest
```

### Run Specific Test Categories

```bash
# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# System tests only
pytest tests/system/
```

### Run Tests by Marker

```bash
# Run only unit tests (with marker)
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only system tests
pytest -m system

# Exclude slow tests
pytest -m "not slow"
```

### Run Specific Test File

```bash
pytest tests/unit/test_physical_rules.py
```

### Run Specific Test Class or Method

```bash
# Run specific test class
pytest tests/unit/test_physical_rules.py::TestSmallFilesRule

# Run specific test method
pytest tests/unit/test_physical_rules.py::TestSmallFilesRule::test_small_files_detected
```

### Verbose Output

```bash
# Show detailed output
pytest -v

# Show stdout/stderr even for passing tests
pytest -v -s
```

### Generate Coverage Report

```bash
# Install coverage
pip install pytest-cov

# Run with coverage
pytest --cov=src --cov-report=html --cov-report=term

# View HTML report
open htmlcov/index.html  # macOS
# or
start htmlcov/index.html  # Windows
```

## Test Fixtures

Shared fixtures are defined in `conftest.py`:

- `mock_spark`: Mock SparkSession for testing
- `sample_alert`: Sample Alert object
- `sample_cluster_context`: Sample ClusterContext
- `sample_suggestion`: Sample Suggestion
- `sample_audit_report`: Sample AuditReport
- `sample_explain_plan`: Sample Catalyst execution plan
- `sample_metrics`: Sample physical metrics
- `sample_policies`: Sample policy configuration
- `mock_dataframe`: Mock DataFrame
- `mock_dbutils`: Mock DBUtils

## Writing New Tests

### Unit Test Template

```python
import pytest
import sys
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.module import ComponentToTest


class TestComponent:
    """Unit tests for Component."""
    
    def test_basic_functionality(self):
        """Test basic functionality."""
        component = ComponentToTest()
        result = component.method()
        
        assert result is not None
    
    def test_edge_case(self):
        """Test edge case handling."""
        component = ComponentToTest()
        result = component.method(edge_case_input)
        
        assert result == expected_output
```

### Integration Test Template

```python
import pytest
from unittest.mock import Mock, MagicMock

from src.module1 import Component1
from src.module2 import Component2


class TestIntegration:
    """Integration tests for Component1 and Component2."""
    
    def test_components_work_together(self):
        """Test components integrate correctly."""
        comp1 = Component1()
        comp2 = Component2()
        
        result = comp1.process(comp2.get_data())
        
        assert result is not None
```

### System Test Template

```python
import pytest

from src.run.main_trigger import APMAutomatedOrchestrator


@pytest.mark.system
class TestEndToEnd:
    """System-level end-to-end tests."""
    
    def test_complete_workflow(self, capsys):
        """Test complete workflow."""
        mock_spark = MagicMock()
        orchestrator = APMAutomatedOrchestrator(mock_spark)
        
        result = orchestrator.run_smart_scan()
        
        assert result["status"] == "FINISHED"
        
        captured = capsys.readouterr()
        assert "expected_output" in captured.out
```

## Test Markers

Use pytest markers to categorize tests:

```python
@pytest.mark.unit
def test_unit_functionality():
    pass

@pytest.mark.integration
def test_integration():
    pass

@pytest.mark.system
def test_end_to_end():
    pass

@pytest.mark.slow
def test_long_running():
    pass

@pytest.mark.requires_spark
def test_with_real_spark():
    pass
```

## Continuous Integration

For CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r tests/requirements.txt
      - name: Run tests
        run: pytest tests/ -v --cov=src
```

## Best Practices

1. **Isolation**: Each test should be independent and not rely on other tests
2. **Naming**: Use descriptive test names that explain what is being tested
3. **Arrange-Act-Assert**: Structure tests with clear setup, execution, and verification
4. **Mocking**: Use mocks for external dependencies (Spark, DBUtils, file systems)
5. **Coverage**: Aim for high test coverage (>80%) of critical paths
6. **Fast Tests**: Keep unit tests fast (<1 second each)
7. **Documentation**: Add docstrings explaining complex test scenarios

## Troubleshooting

### Import Errors

If you encounter import errors:

```bash
# Make sure project root is in PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/path/to/databricks-engine-optimization"
```

Or run pytest from project root:

```bash
cd /path/to/databricks-engine-optimization
pytest tests/
```

### Mock Issues

If mocks aren't working as expected:

1. Check that you're patching the right import path
2. Use `patch.object()` for patching class methods
3. Verify mock return values are correctly set

### Slow Tests

If tests are running slowly:

1. Mark slow tests with `@pytest.mark.slow`
2. Use `pytest -m "not slow"` to skip them during development
3. Review fixture scope (session, module, function)

## Contact

For questions or issues with the test suite, please contact the development team.
