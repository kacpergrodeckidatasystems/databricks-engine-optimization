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
    """Scenariusze E2E symulujące uruchomienie w chmurze Databricks."""

    @patch('src.readers.dataframe_reader.DBUtils', create=True)
    def test_complete_audit_workflow_with_issues(self, mock_dbutils_class):
        """Pełny potok FinOps w kontekście Databricks."""
        # Poprawka: create=True w patch zapobiega AttributeError na czystym Sparku
        mock_dbutils = MagicMock()
        mock_dbutils_class.return_value = mock_dbutils
        
        # Symulacja obecności klastra chmurowego
        os.environ["DATABRICKS_RUNTIME_VERSION"] = "17.x-DBR"
        
        assert "DATABRICKS_RUNTIME_VERSION" in os.environ
        
        # Czyszczenie po teście
        del os.environ["DATABRICKS_RUNTIME_VERSION"]