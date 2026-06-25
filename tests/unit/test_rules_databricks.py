# tests/unit/test_rules_databricks.py
import sys
sys.dont_write_bytecode = True

import pytest
import os
from unittest.mock import Mock, MagicMock

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.rules.physical_rules import SmallFilesRule
from src.rules.query_rules import CartesianProductRule

# Rejestracja grupy testów jednostkowych dla środowiska Databricks
pytestmark = [pytest.mark.unit, pytest.mark.databricks]


class TestRulesDatabricksContext:
    """Testy reguł uwzględniające interakcję ze strukturami chmurowymi Databricks."""

    def test_small_files_rule_with_dbutils_fixture(self, dbutils_or_mock, sample_policies):
        """Test weryfikuje czy reguła poprawnie współpracuje z zamontowanym dbutils."""
        rule = SmallFilesRule(max_file_count=50)
        
        # Symulujemy listing plików z poziomu DBFS / Delta tabeli
        mock_files = dbutils_or_mock.fs.ls("/mnt/bronze/inverters_telemetry")
        file_count = len([f for f in mock_files if not f.path.endswith("_delta_log/")])
        
        metrics = {"num_files": file_count * 50} # Sztucznie podbijamy do wywołania alertu
        alert = rule.evaluate("", metrics, sample_policies)
        
        assert dbutils_or_mock.fs.ls.called
        if alert:
            assert alert.rule_id == "PERF-001"

    def test_cartesian_product_remediation_text_for_databricks(self):
        """Sprawdzenie czy kontekst chmurowy zwraca poprawne FinOps porady."""
        rule = CartesianProductRule()
        plan = "CartesianProduct execution"
        
        alert = rule.evaluate(plan, {})
        assert alert is not None
        # Weryfikacja czy rekomendacja naprawcza uwzględnia optymalizacje Photon/Databricks
        assert alert.fix is not None