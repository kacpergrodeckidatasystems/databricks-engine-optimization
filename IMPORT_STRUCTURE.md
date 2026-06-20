# Import Structure Documentation

## Project Folder Structure

```
src/
├── auditor/              # Core auditing models and engine
│   ├── __init__.py
│   ├── engine.py        # PerformanceEngine
│   └── models.py        # Interfaces: IMetricsReader, IAnalysisRule, IReporter, Alert, etc.
│
├── decorators/          # Reusable decorators
│   └── __init__.py      # trace_execution, safe_execution
│
├── context/             # Environment context providers
│   ├── __init__.py
│   └── environment_provider.py  # EnvironmentProvider, ClusterContext
│
├── finops/              # Financial operations and cost calculation
│   ├── __init__.py
│   └── cost_translator.py       # CostTranslator
│
├── policies/            # Policy management
│   ├── __init__.py
│   └── policy_manager.py        # PolicyManager
│
├── readers/             # Metrics readers
│   ├── __init__.py
│   ├── dataframe_reader.py      # DataFrameExplainReader
│   └── event_log_reader.py      # EventLogReader
│
├── reporters/           # Output reporters
│   ├── __init__.py
│   └── console_reporter.py      # ConsoleReporter
│
├── rules/               # Analysis rules
│   ├── __init__.py
│   └── physical_rules.py        # SmallFilesRule, MissedBroadcastRule
│
└── suggestions/         # Remediation engine and templates
    ├── __init__.py
    ├── remediation_engine.py     # RemediationEngine
    └── suggestions_templates.py  # TEMPLATES dictionary
```

## Correct Import Patterns

### Core Models (from `src.auditor.models`)
All interfaces and data classes are in `src.auditor.models`:

```python
from src.auditor.models import (
    IMetricsReader,    # Interface for metrics readers
    IAnalysisRule,     # Interface for analysis rules
    IReporter,         # Interface for reporters
    Alert,             # Alert data class
    ClusterContext,    # Cluster context data class
    Suggestion,        # Suggestion data class
    AuditReport        # Audit report data class
)
```

### Decorators (from `src.decorators`)
```python
from src.decorators import trace_execution, safe_execution
```

### Other Components
```python
# Engine
from src.auditor.engine import PerformanceEngine

# Readers
from src.readers.dataframe_reader import DataFrameExplainReader
from src.readers.event_log_reader import EventLogReader

# Rules
from src.rules.physical_rules import SmallFilesRule, MissedBroadcastRule

# Policy Management
from src.policies.policy_manager import PolicyManager

# Context
from src.context.environment_provider import EnvironmentProvider

# Suggestions
from src.suggestions.remediation_engine import RemediationEngine
from src.suggestions.suggestions_templates import TEMPLATES

# FinOps
from src.finops.cost_translator import CostTranslator

# Reporters
from src.reporters.console_reporter import ConsoleReporter
```

## Common Import Mistakes (AVOID THESE)

❌ **WRONG:**
```python
from src.auditor.decorators import trace_execution  # decorators NOT in auditor/
from src.auditor.suggestions.templates import TEMPLATES  # suggestions NOT in auditor/
from src.auditor.rules import SmallFilesRule  # rules NOT in auditor/
```

✅ **CORRECT:**
```python
from src.decorators import trace_execution
from src.suggestions.suggestions_templates import TEMPLATES
from src.rules.physical_rules import SmallFilesRule
```

## File-by-File Import Reference

### `src/auditor/engine.py`
```python
from src.auditor.models import IMetricsReader, IAnalysisRule, IReporter, AuditReport
from src.policies.policy_manager import PolicyManager
from src.context.environment_provider import EnvironmentProvider
from src.suggestions.remediation_engine import RemediationEngine
from src.finops.cost_translator import CostTranslator
```

### `src/readers/dataframe_reader.py`
```python
from src.auditor.models import IMetricsReader
from src.decorators import trace_execution, safe_execution
```

### `src/readers/event_log_reader.py`
```python
from src.auditor.models import IMetricsReader
from src.decorators import trace_execution, safe_execution
```

### `src/rules/physical_rules.py`
```python
from src.auditor.models import IAnalysisRule, Alert
```

### `src/context/environment_provider.py`
```python
from src.auditor.models import ClusterContext
```

### `src/suggestions/remediation_engine.py`
```python
from src.auditor.models import Alert, ClusterContext, Suggestion
from src.suggestions.suggestions_templates import TEMPLATES
```

### `src/finops/cost_translator.py`
```python
from src.auditor.models import Alert
```

### `src/reporters/console_reporter.py`
```python
from src.auditor.models import IReporter, AuditReport
```

## Key Principles

1. **`src.auditor.models`** is the central place for all interfaces and data classes
2. **`src.decorators`** (NOT `src.auditor.decorators`) contains reusable decorators
3. **`src.suggestions`** (NOT `src.auditor.suggestions`) contains remediation logic
4. **`src.rules`** contains all analysis rules
5. All other top-level `src/` directories are independent modules

## Verification Command

To verify all imports are correct:

```bash
# Check for incorrect import patterns
grep -r "src.auditor.decorators" src/ && echo "❌ Found wrong decorator imports" || echo "✅ Decorator imports OK"
grep -r "src.auditor.suggestions" src/ && echo "❌ Found wrong suggestions imports" || echo "✅ Suggestions imports OK"
grep -r "src.auditor.rules" src/ && echo "❌ Found wrong rules imports" || echo "✅ Rules imports OK"

# List all import statements
for file in $(find src -name "*.py" -type f | grep -v "__pycache__"); do
  echo "### $file ###"
  grep -E "^from src\.|^import src\." "$file" 2>/dev/null
done
```

## Last Updated

2026-06-19 - All imports verified and corrected