# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Test Suite Overview
# MAGIC %md
# MAGIC # APM Engine Optimization - Test Runner
# MAGIC This notebook runs the pytest test suite for the databricks-engine-optimization project.

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install pytest pytest-cov pytest-mock
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set Up Paths
import sys
import os
import shutil

# Add project root to Python path
project_root = "/Workspace/Users/kacpra.grodeckiego@gmail.com/databricks-engine-optimization"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Copy tests to /tmp (which supports __pycache__) to avoid Workspace filesystem issues
workspace_tests = (
    "/Workspace/Users/kacpra.grodeckiego@gmail.com/databricks-engine-optimization/tests"
)
tmp_tests = "/tmp/databricks-engine-optimization-tests"

# Remove old copy if exists
if os.path.exists(tmp_tests):
    shutil.rmtree(tmp_tests)

# Copy entire tests directory
shutil.copytree(workspace_tests, tmp_tests)

print(f"Project root: {project_root}")
print("Python path configured")
print(f"Tests copied to: {tmp_tests}")

# COMMAND ----------

# DBTITLE 1,Run Unit Tests
import pytest

# Run unit tests from /tmp location
print("=" * 60)
print("Running Unit Tests")
print("=" * 60)

result = pytest.main(["/tmp/databricks-engine-optimization-tests/unit", "-v", "--tb=short", "-ra"])

print(f"\nUnit Tests Result: {'PASSED' if result == 0 else 'FAILED'}")

# COMMAND ----------

# DBTITLE 1,Run Integration Tests
print("=" * 60)
print("Running Integration Tests")
print("=" * 60)

result = pytest.main(
    ["/tmp/databricks-engine-optimization-tests/integration", "-v", "--tb=short", "-ra"]
)

print(f"\nIntegration Tests Result: {'PASSED' if result == 0 else 'FAILED'}")

# COMMAND ----------

# DBTITLE 1,Run System Tests
print("=" * 60)
print("Running System Tests")
print("=" * 60)

result = pytest.main(
    ["/tmp/databricks-engine-optimization-tests/system", "-v", "--tb=short", "-ra"]
)

print(f"\nSystem Tests Result: {'PASSED' if result == 0 else 'FAILED'}")

# COMMAND ----------

# DBTITLE 1,Run All Tests with Coverage
print("=" * 60)
print("Running All Tests with Coverage")
print("=" * 60)

result = pytest.main(
    [
        "/tmp/databricks-engine-optimization-tests",
        "--cov=/Workspace/Users/kacpra.grodeckiego@gmail.com/databricks-engine-optimization/src",
        "--cov-report=term-missing",
        "-v",
    ]
)

print(f"\nAll Tests Result: {'PASSED' if result == 0 else 'FAILED'}")
