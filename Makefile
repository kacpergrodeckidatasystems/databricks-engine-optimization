# =========================================================================
# PROJECT VARIABLES
# =========================================================================
VENV = venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

.PHONY: help venv clean clean-all docker-build docker-up docker-down test package

# Default target displaying help information
help:
	@echo "========================================================================="
	@echo "BESS APM AUDITOR - LOCAL/DEV ENVIRONMENT AUTOMATION (PURE SPARK)"
	@echo "========================================================================="
	@echo "Available commands:"
	@echo "  make venv          - Creates local venv (Py3.11) and installs [dev,test]"
	@echo "  make test          - Runs local unit tests using pytest"
	@echo "  make package       - Builds commercial distribution artifact (.whl)"
	@echo "  make docker-build  - Builds unified PySpark engine image from scratch"
	@echo "  make docker-up     - Runs execution container in the background"
	@echo "  make docker-down   - Stops and removes docker containers"
	@echo "  make clean         - Removes cache and temporary build files"
	@echo "  make clean-all     - Removes what 'clean' does + completely deletes venv"
	@echo "========================================================================="

# =========================================================================
# LOCAL VIRTUAL ENVIRONMENT (VENV)
# =========================================================================
venv:
	@echo "Creating isolated venv based on Python 3.11..."
	python3.11 -m venv $(VENV)
	@echo "Upgrading pip..."
	$(PIP) install --upgrade pip
	@echo "Installing project in editable mode along with [dev,test]..."
	$(PIP) install -e .[dev,test]
	@echo "Venv environment prepared. Activate it: source venv/bin/activate"

# =========================================================================
# UNIT TESTS
# =========================================================================
test:
	@if [ ! -d "$(VENV)" ]; then echo "Error: Missing venv. Run first: make venv"; exit 1; fi
	@echo "Running unit tests inside venv..."
	docker exec -it apm-spark-runner pytest tests/

# =========================================================================
# ARTIFACT BUILDING (.WHL)
# =========================================================================
package:
	@if [ ! -d "$(VENV)" ]; then echo "Error: Missing venv. Run first: make venv"; exit 1; fi
	@echo "Cleaning old packages..."
	rm -rf build/ dist/ *.egg-info
	@echo "Upgrading system builder..."
	$(PIP) install --upgrade build
	@echo "Compiling code to universal Wheel (.whl) locked to Py3.11..."
	$(PYTHON) -m build
	@echo "Artifact built in dist/ directory"

# =========================================================================
# DOCKER-COMPOSE ORCHESTRATION (PURE PYSPARK CORE)
# =========================================================================
docker-build:
	@echo "Building unified PySpark image..."
	docker-compose build --no-cache

build:
	@if [ ! -d "$(VENV)" ]; then echo "Error: Missing venv. Run first: make venv"; exit 1; fi
	@echo "Installing build dependencies..."
	$(VENV)/bin/pip install --upgrade build
	@echo "Compiling the project..."
	$(VENV)/bin/python3 -m build
	@echo "Cleaning up local build artifacts to prevent Docker path pollution..."
	rm -rf *.egg-info  # <-- Ta linia ratuje sytuację przed zanieczyszczeniem wolumenu /app
	@echo "Package successfully generated in dist/ directory!"

publish:
	@if [ ! -d "$(VENV)" ]; then echo "Error: Missing venv. Run first: make venv"; exit 1; fi
	@echo "Upgrading Twine inside venv..."
	$(VENV)/bin/pip install --upgrade twine
	@echo "Checking package integrity & metadata format..."
	$(VENV)/bin/twine check dist/*
	@echo "Uploading distribution packages to PyPI..."
	$(VENV)/bin/twine upload dist/*