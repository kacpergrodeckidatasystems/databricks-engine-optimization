from typing import List
from datetime import datetime
from src.auditor.models import IMetricsReader, IAnalysisRule, IReporter, AuditReport
from src.policies.policy_manager import PolicyManager
from src.context.environment_provider import EnvironmentProvider
from src.suggestions.remediation_engine import RemediationEngine
from src.finops.cost_translator import CostTranslator

class PerformanceEngine:
    def __init__(self, 
                 reader: IMetricsReader, 
                 rules: List[IAnalysisRule], 
                 policy_manager: PolicyManager,
                 env_provider: EnvironmentProvider,
                 remediation_engine: RemediationEngine,
                 cost_translator: CostTranslator,
                 reporter: IReporter):
        self.reader = reader
        self.rules = rules
        self.policy_manager = policy_manager
        self.env_provider = env_provider
        self.remediation_engine = remediation_engine
        self.cost_translator = cost_translator
        self.reporter = reporter

    def run_audit(self, context_name: str = "Proces-ETL-Bronze"):
        plan = self.reader.get_execution_plan()
        metrics = self.reader.get_physical_metrics()
        
        cluster_ctx = self.env_provider.determine_cluster_context()
        finops_policy = self.policy_manager.get_policy("finops")
        
        report = AuditReport(
            context_name=context_name,
            timestamp=datetime.now(),
            cluster_context=cluster_ctx
        )
        
        for rule in self.rules:
            # Dopasowanie sekcji polityki dla konkretnej reguły (nie używane w aktualnej implementacji)
            section_name = "small_files" if "SmallFiles" in rule.__class__.__name__ else "data_skew"
            policy = {section_name: self.policy_manager.get_policy(section_name), "finops": finops_policy}
            
            alert = rule.evaluate(plan, metrics)
            if alert:
                alert.context = context_name
                report.alerts.append(alert)
                
                # Wstrzyknięcie dedykowanej sugestii i wyliczeń kosztów
                suggestion = self.remediation_engine.generate_suggestion(alert, cluster_ctx)
                report.suggestions.append(suggestion)
                
                report.total_estimated_waste_usd += self.cost_translator.calculate_waste(alert)
                
        self.reporter.publish(report)