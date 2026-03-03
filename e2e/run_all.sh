#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}VaultGres E2E Test Suite${NC}"
echo "================================"

cd "$(dirname "$0")"

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    docker compose down -v 2>/dev/null || true
}
trap cleanup EXIT

MODE=${1:-quick}
TEST_NAME=${2:-}

case $MODE in
    quick)
        if [ "$TEST_NAME" = "list" ]; then
            echo -e "${GREEN}Available smoke tests:${NC}"
            cargo test --package e2e --test smoke -- --list | grep ': test$' | sed 's/: test$//'
        elif [ -n "$TEST_NAME" ]; then
            echo -e "${GREEN}Running smoke test: $TEST_NAME${NC}"
            cargo test --package e2e --test smoke $TEST_NAME -- --test-threads=1 --nocapture
        else
            echo -e "${GREEN}Running quick tests (smoke tests)${NC}"
            cargo test --package e2e --test smoke -- --test-threads=1 --nocapture
        fi
        ;;

    scenarios)
        if [ "$TEST_NAME" = "list" ]; then
            echo -e "${GREEN}Available scenarios:${NC}"
            cargo test --package e2e --test scenarios -- --list | grep ': test$' | sed 's/: test$//'
        elif [ -n "$TEST_NAME" ]; then
            echo -e "${GREEN}Running scenario: $TEST_NAME${NC}"
            cargo test --package e2e --test scenarios pet_store::test_$TEST_NAME -- --test-threads=1 --nocapture
        else
            echo -e "${GREEN}Running all scenarios tests${NC}"
            cargo test --package e2e --test scenarios -- --test-threads=1 --nocapture
        fi
        ;;

    pet_store)
        echo -e "${YELLOW}Note: pet_store tests are part of scenarios${NC}"
        echo -e "${YELLOW}Use: ./run_all.sh scenarios pet_store_basic_operations${NC}"
        exit 1
        ;;

    full)
        echo -e "${GREEN}Running full test suite${NC}"
        cargo test --package e2e --test scenarios -- --nocapture
        cargo test --package e2e --test comparison -- --nocapture
        cargo test --package e2e --test load -- --ignored --test-threads=1 --nocapture
        ;;
    
    load)
        echo -e "${GREEN}Running load tests${NC}"
        cargo test --package e2e --test load -- --ignored --test-threads=1 --nocapture
        ;;
    
    soak)
        echo -e "${GREEN}Running soak tests (long duration)${NC}"
        cargo test --package e2e --test soak -- --ignored --test-threads=1 --nocapture
        ;;
    
    compare)
        echo -e "${GREEN}Running comparison benchmarks${NC}"
        docker compose up -d vaultgres postgres
        sleep 10
        cargo test --package e2e --test comparison -- --nocapture
        ;;
    
    monitor)
        echo -e "${GREEN}Starting monitoring stack${NC}"
        docker compose up -d
        echo -e "${YELLOW}Grafana: http://localhost:3000 (admin/admin)${NC}"
        echo -e "${YELLOW}Prometheus: http://localhost:9090${NC}"
        echo -e "${YELLOW}cAdvisor: http://localhost:8080${NC}"
        echo -e "${GREEN}Press Ctrl+C to stop${NC}"
        docker compose logs -f
        ;;
    
    *)
        echo -e "${RED}Unknown mode: $MODE${NC}"
        echo ""
        echo "Usage: $0 [MODE] [OPTIONS]"
        echo ""
        echo "Modes:"
        echo "  quick [test_name]        - Run smoke tests (all or specific)"
        echo "  quick list               - List available smoke tests"
        echo "  scenarios [test_name]    - Run scenario tests (all or specific)"
        echo "  scenarios list           - List available scenarios"
        echo "  full                     - Run full test suite"
        echo "  load                     - Run load tests"
        echo "  soak                     - Run soak tests (24h+)"
        echo "  compare                  - Compare with PostgreSQL"
        echo "  monitor                  - Start monitoring stack"
        echo ""
        echo "Examples:"
        echo "  $0 quick"
        echo "  $0 quick test_basic_create_table"
        echo "  $0 quick list"
        echo "  $0 scenarios"
        echo "  $0 scenarios pet_store_basic_operations"
        echo "  $0 scenarios list"
        exit 1
        ;;
esac

echo -e "${GREEN}Tests completed!${NC}"
