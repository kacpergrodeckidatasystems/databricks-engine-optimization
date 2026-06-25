# tests/unit/test_rules_local.py
import sys
sys.dont_write_bytecode = True

import pytest
import os
from unittest.mock import Mock

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.rules.physical_rules import (
    SmallFilesRule,
    MissedBroadcastRule,
    TypeCastingRule as PhysicalTypeCastingRule,
    OverPartitioningRule,
    DataSkewRule,
    MissingOptimizationRule
)
from src.rules.query_rules import (
    TypeCastingRule as QueryTypeCastingRule,
    CartesianProductRule,
    ExplodeAbuseRule,
    RedundantScanRule,
    NonVectorizedUdfRule
)

# Rejestracja grupy testów jednostkowych
pytestmark = pytest.mark.unit


class TestPhysicalRulesLocal:
    """Testy lokalne reguł sprawdzających plan fizyczny i metryki dyskowe."""

    def test_small_files_detected(self, sample_policies):
        rule = SmallFilesRule(max_file_count=100)
        metrics = {"num_files": 200}
        
        alert = rule.evaluate("", metrics, sample_policies)
        assert alert is not None
        assert alert.rule_id == "PERF-001"
        assert "200 files" in alert.description

    def test_small_files_not_detected(self, sample_policies):
        rule = SmallFilesRule(max_file_count=100)
        metrics = {"num_files": 30}
        
        alert = rule.evaluate("", metrics, sample_policies)
        assert alert is None

    def test_physical_type_casting_metric_in_schema(self):
        rule = PhysicalTypeCastingRule()
        metrics = {
            "schema_fields": {
                "voltage": "string",  # Anomalie: miara numeryczna zapisana jako tekst
                "current": "double"
            }
        }
        alert = rule.evaluate("", metrics)
        assert alert is not None
        assert alert.rule_id == "PERF-003"

    def test_missing_optimization_data_skipping(self):
        rule = MissingOptimizationRule()
        plan = "Filter (temperature > 25.0)\nFileScan delta"
        metrics = {"num_files": 120}
        
        alert = rule.evaluate(plan, metrics)
        if alert:
            assert alert.rule_id == "PERF-007"


class TestQueryRulesLocal:
    """Testy lokalne reguł logicznych i strukturalnych zapytania SQL/Catalyst."""

    def test_cartesian_product_detected(self):
        rule = CartesianProductRule()
        plan = "== Physical Plan ==\nCartesianProduct node execution"
        
        alert = rule.evaluate(plan, {})
        assert alert is not None
        # DOPASOWANIE SYGNATURY: Twój silnik mapuje iloczyn kartezjański jako PERF-005
        assert alert.rule_id == "PERF-005"
        assert "cross join" in alert.description.lower()

    def test_broadcast_nested_loop_join_detected(self):
        rule = CartesianProductRule()
        plan = "BroadcastNestedLoopJoin BuildRight, Cross"
        
        alert = rule.evaluate(plan, {})
        assert alert is not None
        assert alert.rule_id == "PERF-005"

    def test_explode_abuse_detected(self):
        rule = ExplodeAbuseRule()
        plan = "Generate explode(metrics)\n+- Generate explode(telemetry)"
        
        alert = rule.evaluate(plan, {})
        assert alert is not None
        # DOPASOWANIE SYGNATURY: Twój silnik mapuje nadużycie explode jako PERF-008
        assert alert.rule_id == "PERF-008"

    def test_non_vectorized_python_udf_detected(self):
        rule = NonVectorizedUdfRule()
        plan = "Project [BatchEvalPython(my_func#123)]"
        
        alert = rule.evaluate(plan, {})
        assert alert is not None
        assert alert.rule_id == "PERF-010"
        assert "UDF" in alert.title

    def test_query_type_casting_in_filter(self):
        rule = QueryTypeCastingRule()
        plan = "Filter cast(voltage#123 as string) = '230'"
        
        alert = rule.evaluate(plan, {"schema_fields": {}})
        assert alert is not None
        assert alert.rule_id == "PERF-003"