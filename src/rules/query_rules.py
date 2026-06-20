import re
from src.auditor.models import IAnalysisRule, Alert
from typing import Dict, Any, Optional

class TypeCastingRule(IAnalysisRule):
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """Wykrywa numeryczne metryki IoT zdegradowane do typu STRING."""
        detected_anomalies = []
        
        # 1. Sprawdzenie strukturalne na podstawie wstrzykniętego schematu
        schema_fields = metrics.get("schema_fields", {})
        METRIC_KEYWORDS = ["temp", "temperature", "voltage", "soc", "battery", "current", "power", "value"]
        
        for col_name, col_type in schema_fields.items():
            if col_type.lower() == "string":
                if any(keyword in col_name.lower() for keyword in METRIC_KEYWORDS):
                    detected_anomalies.append(f"metryka '{col_name}' zapisana jako STRING")

        # 2. Rezerwowe sprawdzenie aktywnego CAST w filtrach tekstowych
        if plan_text:
            plan_lower = plan_text.lower()
            if "cast(" in plan_lower and any(k in plan_lower for k in ["filter", "datafilters", "photonfilter"]):
                detected_anomalies.append("aktywne rzutowanie typu (CAST) wewnątrz warunku WHERE")

        # Konsolidacja alertu z jawnym przekazaniem parametru 'fix'
        if detected_anomalies:
            return Alert(
                rule_id="PERF-003",
                title="Niewłaściwe typowanie danych (Type Casting & Metric Degradation)",
                description=f"Wykryto krytyczne anomalie typowania: {'; '.join(list(set(detected_anomalies)))}. "
                            f"Całkowicie blokuje to indeksy Delta Lake (Data Skipping), zmuszając klaster do Full Table Scan.",
                severity="WARNING",
                fix="Zapewnij, że typ literału w filtrach odpowiada typowi kolumny. Przebuduj tabelę Bronze, aby metryki IoT były typami numerycznymi (INT/DOUBLE)."
            )
            
        return None