# tests/integration/test_engine_integration_local.py
import sys

sys.dont_write_bytecode = True

import pytest
import os
from unittest.mock import Mock, MagicMock

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.auditor.engine import PerformanceEngine
from src.readers.dataframe_reader import DataFrameExplainReader
from src.rules.physical_rules import SmallFilesRule
from src.rules.query_rules import CartesianProductRule
from src.policies.policy_manager import PolicyManager
from src.suggestions.remediation_engine import RemediationEngine
from src.finops.cost_translator import CostTranslator

# Registration of integration test group for local environment
pytestmark = pytest.mark.integration


class TestEngineIntegrationLocal:
    """Testy integracyjne potoku APM Auditor na lokalnym backendzie Spark."""

    def test_complete_pipeline_with_small_files_violation(
        self, environment_provider, sample_policies, mock_dataframe
    ):
        """
        Test integrates Reader, Engine, Rules and Cost Translator.
        Checks whether exceeding file limit will generate correct FinOps report.
        """
        # 1. Configure Reader with mocked response about plan and high file count (350)
        mock_reader = DataFrameExplainReader(spark=MagicMock(), df=mock_dataframe)
        mock_reader.get_execution_plan = Mock(return_value="== Physical Plan ==\nFileScan parquet")
        mock_reader.get_physical_metrics = Mock(
            return_value={"num_files": 350, "schema_fields": {"id": "int"}}
        )

        # 2. Initialize components with real policies
        policy_manager = PolicyManager(config_dict=sample_policies)
        remediation_engine = RemediationEngine()
        cost_translator = CostTranslator(sample_policies["finops"])

        # Mock reporter to capture final AuditReport object passed to publication
        mock_reporter = Mock()

        # 3. Assemble engine and register local small files rule
        engine = PerformanceEngine(
            reader=mock_reader,
            rules=[SmallFilesRule(max_file_count=100)],
            policy_manager=policy_manager,
            env_provider=environment_provider,
            remediation_engine=remediation_engine,
            cost_translator=cost_translator,
            reporter=mock_reporter,
        )

        # 4. Execute full audit
        engine.run_audit(context_name="integration_bronze_telemetry")

        # 5. Assertion: Check inter-component integration
        assert mock_reporter.publish.called

        # Extract report that was sent to reporter
        report = mock_reporter.publish.call_args[0][0]
        assert report.context_name == "integration_bronze_telemetry"
        assert len(report.alerts) == 1
        assert report.alerts[0].rule_id == "PERF-001"
        assert report.total_estimated_waste_usd > 0.0  # Cost translator calculated FinOps waste

    def test_pipeline_with_multiple_rules_triggered(
        self, environment_provider, sample_policies, mock_dataframe
    ):
        """Weryfikacja integracji silnika przy jednoczesnym wykryciu wielu anomalii."""
        mock_reader = DataFrameExplainReader(spark=MagicMock(), df=mock_dataframe)
        # Plan contains explicit cartesian product, and metrics indicate small files problem
        mock_reader.get_execution_plan = Mock(
            return_value="== Physical Plan ==\nCartesianProduct node"
        )
        mock_reader.get_physical_metrics = Mock(
            return_value={"num_files": 400, "schema_fields": {}}
        )

        mock_reporter = Mock()
        engine = PerformanceEngine(
            reader=mock_reader,
            rules=[SmallFilesRule(max_file_count=100), CartesianProductRule()],
            policy_manager=PolicyManager(config_dict=sample_policies),
            env_provider=environment_provider,
            remediation_engine=RemediationEngine(),
            cost_translator=CostTranslator(sample_policies["finops"]),
            reporter=mock_reporter,
        )

        engine.run_audit(context_name="multi_rule_integration")

        report = mock_reporter.publish.call_args[0][0]
        # Pipeline integrated and aggregated alerts from both different rule classes (Physical + Query)
        assert len(report.alerts) == 2
