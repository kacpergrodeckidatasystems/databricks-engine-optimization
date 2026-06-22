from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from typing import List, Union, Tuple
from dataclasses import field

@dataclass
class Alert:
    rule_id: str
    title: str
    description: str
    fix: str
    severity: str = "WARNING"
    context: str = ""
    metrics_captured: Dict[str, Any] = field(default_factory=dict)

class IMetricsReader(ABC):
    """Interface for components collecting metrics (e.g., from EXPLAIN or Event Logs)."""
    @abstractmethod
    def get_execution_plan(self) -> str:
        pass
        
    @abstractmethod
    def get_physical_metrics(self) -> Dict[str, Any]:
        pass

class IAnalysisRule(ABC):
    """Interface for individual optimization rules (Detectors)."""
    @abstractmethod
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        pass

class IReporter(ABC):
    """Interface for system output layer."""
    @abstractmethod
    def publish(self, alerts: list[Alert]) -> None:
        pass

@dataclass
class ClusterContext:
    is_serverless: bool = False
    aqe_enabled: bool = True
    photon_enabled: bool = False

@dataclass
class Suggestion:
    rule_id: str
    remediation_text: str
    code_template: str

@dataclass
class AuditReport:
    context_name: str
    timestamp: datetime
    cluster_context: ClusterContext
    alerts: List[Alert] = field(default_factory=list)
    suggestions: List[Suggestion] = field(default_factory=list)
    total_estimated_waste_usd: float = 0.0
