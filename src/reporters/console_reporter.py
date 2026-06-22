from src.auditor.models import IReporter, AuditReport

class ConsoleReporter(IReporter):
    def publish(self, report: AuditReport) -> None:
        print("\n" + "="*60)
        print(f"📊 [APM CORE REPORT] Context: {report.context_name}")
        print(f"Timestamp: {report.timestamp.isoformat()}")
        print(f"Estimated FinOps waste: ${report.total_estimated_waste_usd:.4f}")
        print("="*60)
        
        if not report.alerts:
            print("✅ Engine detected no performance anomalies.")
            return
            
        for idx, alert in enumerate(report.alerts):
            print(f"\n[{idx+1}] Alert ID: {alert.rule_id} ({alert.severity})")
            print(f"    Title: {alert.title}")
            print(f"    Description: {alert.description}")
            
            # Match related suggestion
            sug = next((s for s in report.suggestions if s.rule_id == alert.rule_id), None)
            if sug:
                print(f"    💡 Recommendation: {sug.remediation_text}")
                print(f"    💻 Fix Template:\n{sug.code_template}")
        print("\n" + "="*60)
