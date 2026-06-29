from abc import ABC, abstractmethod
from typing import Dict, Any


class IPlanReader(ABC):
    @abstractmethod
    def get_execution_plan(self) -> str:
        """Pobiera tekstowy plan wykonania (EXPLAIN)."""
        pass

    @abstractmethod
    def get_physical_metrics(self) -> Dict[str, Any]:
        """Retrieves volumetrics (number of files, size, schema)."""
        pass


class IEnvironmentProvider(ABC):
    @abstractmethod
    def get_cluster_metadata(self) -> Dict[str, Any]:
        """Retrieves machine configuration, information about AQE, engine, etc."""
        pass
