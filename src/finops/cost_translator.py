from src.auditor.models import Alert
from typing import Dict, Any

class CostTranslator:
    def __init__(self, finops_policy: Dict[str, Any]):
        self.dbu_cost = finops_policy.get("dbu_cost_per_hour", 0.40)
        self.cores = finops_policy.get("estimated_core_count", 8)

    def calculate_waste(self, alert: Alert) -> float:
        """Przelicza narzut techniczny anomalii na realne, szacowane straty finansowe (USD)."""
        if alert.rule_id == "PERF-001":
            # Wykładniczy wzrost kosztu zarządzania metadanymi tysięcy małych plików
            # Próbujemy wydobyć liczbę plików z opisu alerty
            import re
            match = re.search(r"(\d+)\s+plik", alert.description)
            files = int(match.group(1)) if match else 100
            return round((files * 0.0015) * self.dbu_cost, 4)
        elif alert.rule_id == "PERF-002":
            # Narzut na pełny sieciowy Shuffle Exchange małej tabeli
            return round(0.50 * self.dbu_cost, 4)
        return 0.0