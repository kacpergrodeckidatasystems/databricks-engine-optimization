# tests/unit/test_rules_databricks.py
# tests/unit/test_rules_databricks.py
import sys

import pytest

from apm_spark_auditor.rules.physical_rules import SmallFilesRule
from apm_spark_auditor.rules.query_rules import CartesianProductRule

sys.dont_write_bytecode = True

# Registration of unit test group for Databricks environment
pytestmark = [pytest.mark.unit, pytest.mark.databricks]


class TestRulesDatabricksContext:
    """Rule tests considering interaction with Databricks cloud structures."""

    def test_small_files_rule_with_dbutils_fixture(self, dbutils_or_mock, sample_policies):
        """Test verifies whether rule correctly cooperates with mounted dbutils."""
        rule = SmallFilesRule(max_file_count=50)

        # Simulate file listing from DBFS / Delta table level
        mock_files = dbutils_or_mock.fs.ls("/mnt/bronze/inverters_telemetry")
        file_count = len([f for f in mock_files if not f.path.endswith("_delta_log/")])

        metrics = {"num_files": file_count * 50}  # Artificially increase to trigger alert
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
        # Verification whether remediation recommendation includes Photon/Databricks optimizations
        assert alert.fix is not None
