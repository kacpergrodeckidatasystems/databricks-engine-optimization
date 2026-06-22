#!/bin/bash
# Test runner script for APM Engine Optimization

set -e

echo "========================================"
echo "APM Engine Optimization - Test Suite"
echo "========================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to run tests with a label
run_test_suite() {
    local label=$1
    local path=$2
    local marker=$3
    
    echo -e "${YELLOW}Running $label...${NC}"
    if [ -n "$marker" ]; then
        pytest "$path" -m "$marker" -v
    else
        pytest "$path" -v
    fi
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $label passed${NC}"
    else
        echo -e "${RED}✗ $label failed${NC}"
        exit 1
    fi
    echo ""
}

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${RED}Error: pytest is not installed${NC}"
    echo "Install it with: pip install -r tests/requirements.txt"
    exit 1
fi

# Parse command line arguments
TEST_TYPE=${1:-all}

case $TEST_TYPE in
    unit)
        echo "Running Unit Tests Only"
        run_test_suite "Unit Tests" "tests/unit/" "unit"
        ;;
    
    integration)
        echo "Running Integration Tests Only"
        run_test_suite "Integration Tests" "tests/integration/" "integration"
        ;;
    
    system)
        echo "Running System Tests Only"
        run_test_suite "System Tests" "tests/system/" "system"
        ;;
    
    fast)
        echo "Running Fast Tests (excluding slow tests)"
        pytest tests/ -m "not slow" -v
        ;;
    
    coverage)
        echo "Running All Tests with Coverage"
        pytest tests/ --cov=src --cov-report=html --cov-report=term -v
        echo ""
        echo -e "${GREEN}Coverage report generated in htmlcov/index.html${NC}"
        ;;
    
    all)
        echo "Running All Tests"
        run_test_suite "Unit Tests" "tests/unit/"
        run_test_suite "Integration Tests" "tests/integration/"
        run_test_suite "System Tests" "tests/system/"
        
        echo -e "${GREEN}========================================${NC}"
        echo -e "${GREEN}All test suites passed successfully!${NC}"
        echo -e "${GREEN}========================================${NC}"
        ;;
    
    *)
        echo -e "${RED}Unknown test type: $TEST_TYPE${NC}"
        echo ""
        echo "Usage: ./run_tests.sh [test_type]"
        echo ""
        echo "Test types:"
        echo "  unit        - Run unit tests only"
        echo "  integration - Run integration tests only"
        echo "  system      - Run system tests only"
        echo "  fast        - Run fast tests (excluding slow)"
        echo "  coverage    - Run all tests with coverage report"
        echo "  all         - Run all test suites (default)"
        exit 1
        ;;
esac
