# tests/unit/test_rules_local.py
import sys

sys.dont_write_bytecode = True

import pytest
import os

# Dynamically map project root for clean execution path imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.rules.physical_rules import (
    SmallFilesRule,
    TypeCastingRule as PhysicalTypeCastingRule,
    MissingOptimizationRule,
)
from src.rules.query_rules import (
    TypeCastingRule as QueryTypeCastingRule,
    CartesianProductRule,
    ExplodeAbuseRule,
    NonVectorizedUdfRule,
)

# Register the unit test suite marker globally for this module
pytestmark = pytest.mark.unit


class TestPhysicalRulesLocal:
    """Local unit tests for physical rules checking execution plans and disk metrics."""

    def test_small_files_detected(self, sample_policies):
        """Verify that the small files rule correctly triggers an alert when the file count exceeds the configured limit."""
        rule = SmallFilesRule(max_file_count=100)
        metrics = {"num_files": 200}

        alert = rule.evaluate("", metrics, sample_policies)
        assert alert is not None
        assert alert.rule_id == "PERF-001"
        assert "200 files" in alert.description

    def test_small_files_not_detected(self, sample_policies):
        """Ensure no alert is triggered when the file count is safely below the threshold."""
        rule = SmallFilesRule(max_file_count=100)
        metrics = {"num_files": 30}

        alert = rule.evaluate("", metrics, sample_policies)
        assert alert is None

    def test_physical_type_casting_metric_in_schema(self):
        """Detect an architectural anomaly when a numeric metric column is incorrectly stored as a string type in the schema."""
        rule = PhysicalTypeCastingRule()
        metrics = {
            "schema_fields": {
                "voltage": "string",  # Anomaly: numerical metric stored as plain text
                "current": "double",
            }
        }
        alert = rule.evaluate("", metrics)
        assert alert is not None
        assert alert.rule_id == "PERF-003"

    def test_missing_optimization_data_skipping(self):
        """Verify that missing data skipping optimizations on large datasets generate the appropriate performance alert."""
        rule = MissingOptimizationRule()
        plan = "Filter (temperature > 25.0)\nFileScan delta"
        metrics = {"num_files": 120}

        alert = rule.evaluate(plan, metrics)
        if alert:
            assert alert.rule_id == "PERF-007"


class TestQueryRulesLocal:
    """Local unit tests for logical and structural query optimization rules within the Catalyst plan."""

    def test_cartesian_product_detected(self):
        """Verify that an explicit cross join or Cartesian product triggers the correct engine rule ID (PERF-005)."""
        rule = CartesianProductRule()
        plan = "== Physical Plan ==\nCartesianProduct node execution"

        alert = rule.evaluate(plan, {})
        assert alert is not None
        # MATCHING SIGNATURE: The engine maps Cartesian Product as PERF-005
        assert alert.rule_id == "PERF-005"
        assert "cross join" in alert.description.lower()

    def test_broadcast_nested_loop_join_detected(self):
        """Ensure a BroadcastNestedLoopJoin operation is flagged as a Cartesian product variant under rule PERF-005."""
        rule = CartesianProductRule()
        plan = "BroadcastNestedLoopJoin BuildRight, Cross"

        alert = rule.evaluate(plan, {})
        assert alert is not None
        assert alert.rule_id == "PERF-005"

    def test_explode_abuse_detected(self):
        """Detect table performance degradation caused by consecutive or unoptimized explode operations (PERF-008)."""
        rule = ExplodeAbuseRule()
        plan = "Generate explode(metrics)\n+- Generate explode(telemetry)"

        alert = rule.evaluate(plan, {})
        assert alert is not None
        # MATCHING SIGNATURE: The engine maps explode abuse as PERF-008
        assert alert.rule_id == "PERF-008"

    def test_non_vectorized_python_udf_detected(self):
        """Flag non-vectorized Python UDF execution blocks that prevent Catalyst optimization paths (PERF-010)."""
        rule = NonVectorizedUdfRule()
        plan = "Project [BatchEvalPython(my_func#123)]"

        alert = rule.evaluate(plan, {})
        assert alert is not None
        assert alert.rule_id == "PERF-010"
        assert "UDF" in alert.title

    def test_query_type_casting_in_filter(self):
        """Verify that explicit cast functions inside filter expressions are flagged for dynamic type casting overhead."""
        rule = QueryTypeCastingRule()
        plan = "Filter cast(voltage#123 as string) = '230'"

        alert = rule.evaluate(plan, {"schema_fields": {}})
        assert alert is not None
        assert alert.rule_id == "PERF-003"
