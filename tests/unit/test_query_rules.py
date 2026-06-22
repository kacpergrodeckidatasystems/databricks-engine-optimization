# Disable bytecode caching for Databricks Workspace
import sys
sys.dont_write_bytecode = True

import pytest
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.rules.query_rules import (
    TypeCastingRule,
    CartesianProductRule,
    ExplodeAbuseRule,
    RedundantScanRule,
    NonVectorizedUdfRule
)


class TestTypeCastingRuleQuery:
    """Unit tests for TypeCastingRule (query version)."""
    
    def test_cast_in_filter(self):
        """Test detection of CAST in WHERE clause."""
        rule = TypeCastingRule()
        plan = "Filter cast(temperature#123 as string) = '25'"
        metrics = {"schema_fields": {}}
        
        alert = rule.evaluate(plan, metrics)
        
        assert alert is not None
        assert alert.rule_id == "PERF-003"
        assert "casting" in alert.description.lower()
    
    def test_string_metrics_detected(self):
        """Test detection of metrics stored as STRING."""
        rule = TypeCastingRule()
        metrics = {
            "schema_fields": {
                "voltage": "string",
                "current": "string"
            }
        }
        
        alert = rule.evaluate("", metrics)
        
        assert alert is not None
        assert "voltage" in alert.description or "current" in alert.description


class TestCartesianProductRule:
    """Unit tests for CartesianProductRule."""
    
    def test_cartesian_product_detected(self):
        """Test detection of Cartesian product."""
        rule = CartesianProductRule()
        plan = """
        CartesianProduct
        :- Scan table1
        +- Scan table2
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-005"
        assert alert.severity == "CRITICAL"
        assert "Cartesian" in alert.title
    
    def test_broadcast_nested_loop_detected(self):
        """Test detection of BroadcastNestedLoopJoin."""
        rule = CartesianProductRule()
        plan = "BroadcastNestedLoopJoin Inner BuildRight"
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-005"
    
    def test_normal_join_no_alert(self):
        """Test no alert for normal joins."""
        rule = CartesianProductRule()
        plan = "BroadcastHashJoin [id#123]"
        
        alert = rule.evaluate(plan, {})
        
        assert alert is None
    
    def test_empty_plan_no_alert(self):
        """Test no alert for empty plan."""
        rule = CartesianProductRule()
        
        alert = rule.evaluate(None, {})
        
        assert alert is None


class TestExplodeAbuseRule:
    """Unit tests for ExplodeAbuseRule."""
    
    def test_explode_detected(self):
        """Test detection of explode() usage."""
        rule = ExplodeAbuseRule()
        plan = """
        Generate explode(array_column#123)
        +- FileScan
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-008"
        assert "explode" in alert.title.lower()
        assert alert.severity == "WARNING"
    
    def test_no_explode_no_alert(self):
        """Test no alert when explode not used."""
        rule = ExplodeAbuseRule()
        plan = "Select id, name FROM table"
        
        alert = rule.evaluate(plan, {})
        
        assert alert is None
    
    def test_empty_plan_no_alert(self):
        """Test no alert for empty plan."""
        rule = ExplodeAbuseRule()
        
        alert = rule.evaluate(None, {})
        
        assert alert is None


class TestRedundantScanRule:
    """Unit tests for RedundantScanRule."""
    
    def test_multiple_scans_detected(self):
        """Test detection of multiple Delta scans."""
        rule = RedundantScanRule()
        plan = """
        Scan delta [id#123]
        +- Scan delta [id#456]
        +- Scan delta [id#789]
        +- Scan delta [id#999]
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-009"
        assert "4 times" in alert.description
        assert alert.severity == "WARNING"
    
    def test_few_scans_no_alert(self):
        """Test no alert when scan count is low."""
        rule = RedundantScanRule()
        plan = """
        Scan delta [id#123]
        Scan delta [id#456]
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is None
    
    def test_empty_plan_no_alert(self):
        """Test no alert for empty plan."""
        rule = RedundantScanRule()
        
        alert = rule.evaluate(None, {})
        
        assert alert is None


class TestNonVectorizedUdfRule:
    """Unit tests for NonVectorizedUdfRule."""
    
    def test_python_udf_detected(self):
        """Test detection of Python UDF."""
        rule = NonVectorizedUdfRule()
        plan = """
        Project [BatchEvalPython(my_function)]
        +- FileScan
        """
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-010"
        assert "UDF" in alert.title
        assert alert.severity == "HIGH"
    
    def test_scala_udf_detected(self):
        """Test detection of Scala UDF."""
        rule = NonVectorizedUdfRule()
        plan = "ScalaUDF(customFunction)"
        
        alert = rule.evaluate(plan, {})
        
        assert alert is not None
        assert alert.rule_id == "PERF-010"
    
    def test_builtin_functions_no_alert(self):
        """Test no alert for built-in functions."""
        rule = NonVectorizedUdfRule()
        plan = "Project [upper(name#123), concat(first, last)]"
        
        alert = rule.evaluate(plan, {})
        
        assert alert is None
    
    def test_empty_plan_no_alert(self):
        """Test no alert for empty plan."""
        rule = NonVectorizedUdfRule()
        
        alert = rule.evaluate(None, {})
        
        assert alert is None
