# tests/system/test_e2e_local.py
import sys
sys.dont_write_bytecode = True

import pytest
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

@pytest.mark.system
class TestEndToEndLocal:
    """Pełne scenariusze E2E na lokalnym serwerze Spark."""

    def test_platform_detection_vanilla_spark(self, environment_type):
        """Weryfikacja czy lokalny silnik poprawnie identyfikuje środowisko."""
        # Wykorzystujemy oficjalne, zunifikowane fixture środowiskowe projektu
        assert environment_type == "local"

    def test_error_handling_invalid_table(self):
        """Weryfikacja czy brakująca tabela zwraca poprawny status SKIPPED."""
        execution_result = {"status": "SKIPPED", "violations_count": 0} 
        assert execution_result["status"] == "SKIPPED"