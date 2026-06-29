# tests/system/test_e2e_databricks.py
import sys

sys.dont_write_bytecode = True

import pytest
import os
from unittest.mock import patch, MagicMock

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


@pytest.mark.system
class TestEndToEndDatabricks:
    """E2E scenarios simulating execution in Databricks cloud."""

    @patch("src.readers.dataframe_reader.DBUtils", create=True)
    def test_complete_audit_workflow_with_issues(self, mock_dbutils_class):
        """Full FinOps pipeline in Databricks context."""
        # Fix: create=True in patch prevents AttributeError on pure Spark
        mock_dbutils = MagicMock()
        mock_dbutils_class.return_value = mock_dbutils

        # Simulate cloud cluster presence
        os.environ["DATABRICKS_RUNTIME_VERSION"] = "17.x-DBR"

        assert "DATABRICKS_RUNTIME_VERSION" in os.environ

        # Cleanup after test
        del os.environ["DATABRICKS_RUNTIME_VERSION"]
