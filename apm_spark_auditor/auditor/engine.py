from typing import List
from datetime import datetime
from apm_spark_auditor.auditor.models import IMetricsReader, IAnalysisRule, IReporter, AuditReport
from apm_spark_auditor.policies.policy_manager import PolicyManager
from apm_spark_auditor.context.environment_provider import EnvironmentProvider
from apm_spark_auditor.suggestions.remediation_engine import RemediationEngine
from apm_spark_auditor.finops.cost_translator import CostTranslator


class PerformanceEngine:
    def __init__(
        self,
        reader: IMetricsReader,
        rules: List[IAnalysisRule],
        policy_manager: PolicyManager,
        env_provider: EnvironmentProvider,
        remediation_engine: RemediationEngine,
        cost_translator: CostTranslator,
        reporter: IReporter,
    ):
        self.reader = reader
        self.rules = rules
        self.policy_manager = policy_manager
        self.env_provider = env_provider
        self.remediation_engine = remediation_engine
        self.cost_translator = cost_translator
        self.reporter = reporter

    def run_audit(self, context_name: str = "ETL-Bronze-Process"):
        plan = self.reader.get_execution_plan()
        metrics = self.reader.get_physical_metrics()

        cluster_ctx = self.env_provider.determine_cluster_context()
        finops_policy = self.policy_manager.get_policy("finops")

        report = AuditReport(
            context_name=context_name, timestamp=datetime.now(), cluster_context=cluster_ctx
        )

        for rule in self.rules:
            # Policy section matching for specific rule (not used in current implementation)
            section_name = "small_files" if "SmallFiles" in rule.__class__.__name__ else "data_skew"
            policy = {
                section_name: self.policy_manager.get_policy(section_name),
                "finops": finops_policy,
            }

            alert = rule.evaluate(plan, metrics, policy)
            if alert:
                alert.context = context_name
                report.alerts.append(alert)

                # Inject dedicated suggestion and cost calculations
                suggestion = self.remediation_engine.generate_suggestion(alert, cluster_ctx)
                report.suggestions.append(suggestion)

                report.total_estimated_waste_usd += self.cost_translator.calculate_waste(alert)

        self.reporter.publish(report)
