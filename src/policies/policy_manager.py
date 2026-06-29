from typing import Dict, Any


class PolicyManager:
    def __init__(self, config_dict: Dict[str, Any] = None):
        # Default policies, in case YAML file is missing in cloud environment
        self._policies = config_dict or {
            "small_files": {"max_file_count": 100, "threshold_size_mb": 10.0},
            "data_skew": {"max_duration_ratio": 5.0},
            "over_partitioning": {"max_partitions": 1000},
            "finops": {"dbu_cost_per_hour": 0.40, "estimated_core_count": 8},
        }

    def get_policy(self, section: str) -> Dict[str, Any]:
        return self._policies.get(section, {})
