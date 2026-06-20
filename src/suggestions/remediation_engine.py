from src.auditor.models import Alert, ClusterContext, Suggestion
from src.suggestions.suggestions_templates import TEMPLATES

class RemediationEngine:
    def generate_suggestion(self, alert: Alert, cluster_ctx: ClusterContext) -> Suggestion:
        """Buduje spersonalizowaną podpowiedź naprawczą w oparciu o stan techniczny klastra."""
        template = TEMPLATES.get(alert.rule_id, {
            "text": "Brak predefiniowanej sugestii automatycznej.",
            "code": "# Przeanalizuj problem manualnie."
        })
        
        remediation_text = template["text"]
        code_template = template["code"]
        
        # Kontekstowa modyfikacja podpowiedzi na bazie parametrów klastra
        if alert.rule_id == "PERF-001" and cluster_ctx.is_serverless:
            remediation_text += " [Uwaga FinOps: Na klastrach Serverless kompaktowanie wykonuj przyrostowo!]"
            
        return Suggestion(
            rule_id=alert.rule_id,
            remediation_text=remediation_text,
            code_template=code_template
        )