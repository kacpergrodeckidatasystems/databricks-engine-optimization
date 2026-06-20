from src.auditor.models import IReporter, AuditReport

class ConsoleReporter(IReporter):
    def publish(self, report: AuditReport) -> None:
        print("\n" + "="*60)
        print(f"📊 [APM CORE REPORT] Kontekst: {report.context_name}")
        print(f"Timestamp: {report.timestamp.isoformat()}")
        print(f"Szacowane straty FinOps: ${report.total_estimated_waste_usd:.4f}")
        print("="*60)
        
        if not report.alerts:
            print("✅ Silnik nie wykrył żadnych anomalii wydajnościowych.")
            return
            
        for idx, alert in enumerate(report.alerts):
            print(f"\n[{idx+1}] Alert ID: {alert.rule_id} ({alert.severity})")
            print(f"    Tytuł: {alert.title}")
            print(f"    Opis : {alert.description}")
            
            # Dopasowanie powiązanej sugestii
            sug = next((s for s in report.suggestions if s.rule_id == alert.rule_id), None)
            if sug:
                print(f"    💡 Rekomendacja: {sug.remediation_text}")
                print(f"    💻 Szablon Fixa:\n{sug.code_template}")
        print("\n" + "="*60)