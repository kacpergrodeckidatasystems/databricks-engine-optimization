from apm_spark_auditor.auditor.models import Alert
from typing import Dict, Any


class CostTranslator:
    def __init__(self, finops_policy: Dict[str, Any]):
        self.dbu_cost = finops_policy.get("dbu_cost_per_hour", 0.40)
        self.cores = finops_policy.get("estimated_core_count", 8)

    def calculate_waste(self, alert: Alert) -> float:
        """Converts technical overhead of anomaly to real, estimated financial losses (USD)."""
        if alert.rule_id == "PERF-001":
            # Exponential increase in metadata management cost for thousands of small files
            # Try to extract file count from alert description
            import re

            match = re.search(r"(\d+)\s+file", alert.description)
            files = int(match.group(1)) if match else 100
            return round((files * 0.0015) * self.dbu_cost, 4)
        elif alert.rule_id == "PERF-002":
            # Overhead for full network Shuffle Exchange of small table
            return round(0.50 * self.dbu_cost, 4)
        return 0.0
