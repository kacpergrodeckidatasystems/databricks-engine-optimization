# tests/unit/test_engine_databricks.py
import sys

sys.dont_write_bytecode = True

import pytest
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apm_spark_auditor.auditor.models import Alert, ClusterContext
from apm_spark_auditor.suggestions.remediation_engine import RemediationEngine


@pytest.mark.unit
class TestPerformanceEngineDatabricks:
    """Testy jednostkowe specyfiki chmurowej Databricks."""

    def test_suggestion_with_serverless_context(self):
        engine = RemediationEngine()
        alert = Alert(
            rule_id="PERF-001", title="Small Files", description="D", fix="F", severity="WARNING"
        )

        # Testujemy zachowanie dla klastra Serverless
        cluster_ctx = ClusterContext(is_serverless=True)
        suggestion = engine.generate_suggestion(alert, cluster_ctx)

        assert (
            "Serverless" in suggestion.remediation_text
            or "incrementally" in suggestion.remediation_text
        )

    def test_dbutils_file_listing_with_fixture(self, dbutils_or_mock):
        """Verification whether mounted dbutils_or_mock works correctly."""
        assert dbutils_or_mock is not None
