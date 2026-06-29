# src/__init__.py
from apm_spark_auditor.run.main_trigger import APMAutomatedOrchestrator as APMAuditor

# Definiujemy publiczne API paczki
__all__ = ["APMAuditor"]