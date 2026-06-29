from apm_spark_auditor.auditor.models import Alert, ClusterContext, Suggestion
from apm_spark_auditor.suggestions.suggestions_templates import TEMPLATES


class RemediationEngine:
    def generate_suggestion(self, alert: Alert, cluster_ctx: ClusterContext) -> Suggestion:
        """Builds personalized remediation hint based on cluster technical state."""
        template = TEMPLATES.get(
            alert.rule_id,
            {
                "text": "No predefined automatic suggestion available.",
                "code": "# Analyze problem manually.",
            },
        )

        remediation_text = template["text"]
        code_template = template["code"]

        # Contextual suggestion modification based on cluster parameters
        if alert.rule_id == "PERF-001" and cluster_ctx.is_serverless:
            remediation_text += (
                " [FinOps Note: On Serverless clusters, perform compaction incrementally!]"
            )

        return Suggestion(
            rule_id=alert.rule_id, remediation_text=remediation_text, code_template=code_template
        )
