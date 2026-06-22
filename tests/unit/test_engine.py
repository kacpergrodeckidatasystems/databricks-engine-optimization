# Disable bytecode caching for Databricks Workspace
import sys
sys.dont_write_bytecode = True

import pytest
import os
from unittest.mock import Mock, MagicMock

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.auditor.engine import PerformanceEngine
from src.auditor.models import Alert, ClusterContext
from src.rules.physical_rules import SmallFilesRule
from src.policies.policy_manager import PolicyManager
from src.context.environment_provider import EnvironmentProvider
from src.suggestions.remediation_engine import RemediationEngine
from src.finops.cost_translator import CostTranslator
from src.reporters.console_reporter import ConsoleReporter


class TestPerformanceEngine:
    """Unit tests for PerformanceEngine."""
    
    def test_engine_initialization(self):
        """Test engine initializes correctly."""
        # Create mock reader
        mock_reader = Mock()
        mock_reader.get_execution_plan = Mock(return_value="")
        mock_reader.get_physical_metrics = Mock(return_value={})
        
        # Create real dependencies
        policy_manager = PolicyManager()
        mock_spark = Mock()
        env_provider = EnvironmentProvider(mock_spark)
        remediation_engine = RemediationEngine()
        cost_translator = CostTranslator(policy_manager.get_policy("finops"))
        reporter = ConsoleReporter()
        
        # Create engine
        engine = PerformanceEngine(
            reader=mock_reader,
            rules=[SmallFilesRule()],
            policy_manager=policy_manager,
            env_provider=env_provider,
            remediation_engine=remediation_engine,
            cost_translator=cost_translator,
            reporter=reporter
        )
        
        assert engine is not None
        assert len(engine.rules) == 1
    
    def test_engine_run_audit_with_alerts(self, capsys):
        """Test engine runs audit and generates alerts."""
        # Create mock reader that returns data triggering alerts
        mock_reader = Mock()
        mock_reader.get_execution_plan = Mock(return_value="")
        mock_reader.get_physical_metrics = Mock(return_value={"num_files": 200})
        
        # Create dependencies
        policy_manager = PolicyManager()
        mock_spark = Mock()
        mock_spark.conf.get = Mock(return_value="true")
        env_provider = EnvironmentProvider(mock_spark)
        remediation_engine = RemediationEngine()
        cost_translator = CostTranslator(policy_manager.get_policy("finops"))
        reporter = ConsoleReporter()
        
        # Create engine with SmallFilesRule
        engine = PerformanceEngine(
            reader=mock_reader,
            rules=[SmallFilesRule(max_file_count=100)],
            policy_manager=policy_manager,
            env_provider=env_provider,
            remediation_engine=remediation_engine,
            cost_translator=cost_translator,
            reporter=reporter
        )
        
        # Run audit
        engine.run_audit(context_name="test_context")
        
        # Check output contains alert information
        captured = capsys.readouterr()
        assert "PERF-001" in captured.out
        assert "200 files" in captured.out
    
    def test_engine_run_audit_no_alerts(self, capsys):
        """Test engine runs audit with no alerts."""
        # Create mock reader with clean data
        mock_reader = Mock()
        mock_reader.get_execution_plan = Mock(return_value="")
        mock_reader.get_physical_metrics = Mock(return_value={"num_files": 50})
        
        # Create dependencies
        policy_manager = PolicyManager()
        mock_spark = Mock()
        env_provider = EnvironmentProvider(mock_spark)
        remediation_engine = RemediationEngine()
        cost_translator = CostTranslator(policy_manager.get_policy("finops"))
        reporter = ConsoleReporter()
        
        # Create engine
        engine = PerformanceEngine(
            reader=mock_reader,
            rules=[SmallFilesRule(max_file_count=100)],
            policy_manager=policy_manager,
            env_provider=env_provider,
            remediation_engine=remediation_engine,
            cost_translator=cost_translator,
            reporter=reporter
        )
        
        # Run audit
        engine.run_audit(context_name="test_context")
        
        # Check output indicates no anomalies
        captured = capsys.readouterr()
        assert "no performance anomalies" in captured.out.lower()
    
    def test_engine_multiple_rules(self):
        """Test engine with multiple rules."""
        from src.rules.physical_rules import MissedBroadcastRule
        
        mock_reader = Mock()
        mock_reader.get_execution_plan = Mock(return_value="SortMergeJoin Exchange")
        mock_reader.get_physical_metrics = Mock(return_value={"num_files": 200})
        
        policy_manager = PolicyManager()
        mock_spark = Mock()
        env_provider = EnvironmentProvider(mock_spark)
        remediation_engine = RemediationEngine()
        cost_translator = CostTranslator(policy_manager.get_policy("finops"))
        reporter = ConsoleReporter()
        
        # Create engine with multiple rules
        engine = PerformanceEngine(
            reader=mock_reader,
            rules=[SmallFilesRule(), MissedBroadcastRule()],
            policy_manager=policy_manager,
            env_provider=env_provider,
            remediation_engine=remediation_engine,
            cost_translator=cost_translator,
            reporter=reporter
        )
        
        assert len(engine.rules) == 2


class TestPolicyManager:
    """Unit tests for PolicyManager."""
    
    def test_default_policies_loaded(self):
        """Test default policies are loaded correctly."""
        manager = PolicyManager()
        
        small_files_policy = manager.get_policy("small_files")
        assert small_files_policy is not None
        assert "max_file_count" in small_files_policy
        assert small_files_policy["max_file_count"] == 100
    
    def test_finops_policy(self):
        """Test FinOps policy retrieval."""
        manager = PolicyManager()
        
        finops_policy = manager.get_policy("finops")
        assert finops_policy is not None
        assert "dbu_cost_per_hour" in finops_policy
        assert finops_policy["dbu_cost_per_hour"] == 0.40
    
    def test_custom_policies(self):
        """Test custom policy configuration."""
        custom_config = {
            "small_files": {"max_file_count": 500},
            "finops": {"dbu_cost_per_hour": 1.00}
        }
        
        manager = PolicyManager(config_dict=custom_config)
        
        small_files_policy = manager.get_policy("small_files")
        assert small_files_policy["max_file_count"] == 500
        
        finops_policy = manager.get_policy("finops")
        assert finops_policy["dbu_cost_per_hour"] == 1.00
    
    def test_missing_policy_returns_empty(self):
        """Test missing policy returns empty dict."""
        manager = PolicyManager()
        
        policy = manager.get_policy("nonexistent_policy")
        assert policy == {}


class TestCostTranslator:
    """Unit tests for CostTranslator."""
    
    def test_small_files_cost_calculation(self):
        """Test cost calculation for small files problem."""
        finops_policy = {"dbu_cost_per_hour": 0.40, "estimated_core_count": 8}
        translator = CostTranslator(finops_policy)
        
        alert = Alert(
            rule_id="PERF-001",
            title="Small files problem",
            description="Table contains 200 files, exceeding threshold.",
            fix="Run OPTIMIZE",
            severity="WARNING"
        )
        
        cost = translator.calculate_waste(alert)
        
        assert cost > 0
        assert isinstance(cost, float)
    
    def test_broadcast_join_cost_calculation(self):
        """Test cost calculation for missed broadcast join."""
        finops_policy = {"dbu_cost_per_hour": 0.40, "estimated_core_count": 8}
        translator = CostTranslator(finops_policy)
        
        alert = Alert(
            rule_id="PERF-002",
            title="Missed broadcast",
            description="Detected shuffle join.",
            fix="Use broadcast()",
            severity="WARNING"
        )
        
        cost = translator.calculate_waste(alert)
        
        assert cost > 0
        assert cost == round(0.50 * 0.40, 4)
    
    def test_unknown_rule_returns_zero(self):
        """Test unknown rule returns zero cost."""
        finops_policy = {"dbu_cost_per_hour": 0.40}
        translator = CostTranslator(finops_policy)
        
        alert = Alert(
            rule_id="PERF-999",
            title="Unknown",
            description="Unknown problem.",
            fix="Unknown fix",
            severity="WARNING"
        )
        
        cost = translator.calculate_waste(alert)
        
        assert cost == 0.0


class TestRemediationEngine:
    """Unit tests for RemediationEngine."""
    
    def test_generate_suggestion_for_small_files(self):
        """Test suggestion generation for small files rule."""
        engine = RemediationEngine()
        
        alert = Alert(
            rule_id="PERF-001",
            title="Small files",
            description="Too many files.",
            fix="Run OPTIMIZE",
            severity="WARNING"
        )
        
        cluster_ctx = ClusterContext(is_serverless=False)
        
        suggestion = engine.generate_suggestion(alert, cluster_ctx)
        
        assert suggestion is not None
        assert suggestion.rule_id == "PERF-001"
        assert "OPTIMIZE" in suggestion.code_template
    
    def test_suggestion_with_serverless_context(self):
        """Test suggestion modified for serverless context."""
        engine = RemediationEngine()
        
        alert = Alert(
            rule_id="PERF-001",
            title="Small files",
            description="Too many files.",
            fix="Run OPTIMIZE",
            severity="WARNING"
        )
        
        cluster_ctx = ClusterContext(is_serverless=True)
        
        suggestion = engine.generate_suggestion(alert, cluster_ctx)
        
        assert "Serverless" in suggestion.remediation_text or "incrementally" in suggestion.remediation_text
    
    def test_unknown_rule_gets_default_suggestion(self):
        """Test unknown rule gets default suggestion."""
        engine = RemediationEngine()
        
        alert = Alert(
            rule_id="PERF-999",
            title="Unknown",
            description="Unknown problem.",
            fix="Unknown",
            severity="WARNING"
        )
        
        cluster_ctx = ClusterContext()
        
        suggestion = engine.generate_suggestion(alert, cluster_ctx)
        
        assert suggestion is not None
        # Default suggestion contains "automatic" in text and "manually" in code
        assert "automatic" in suggestion.remediation_text.lower()
        assert "manually" in suggestion.code_template.lower()
