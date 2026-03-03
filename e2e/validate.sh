#!/bin/bash
set -e

echo "E2E Framework Validation"
echo "========================"

cd "$(dirname "$0")"

echo "✓ Checking directory structure..."
test -f STRATEGY.md && echo "  - STRATEGY.md exists"
test -f docker-compose.yml && echo "  - docker-compose.yml exists"
test -f lib.rs && echo "  - lib.rs exists"
test -d scenarios && echo "  - scenarios/ exists"
test -d load && echo "  - load/ exists"
test -d soak && echo "  - soak/ exists"
test -d comparison && echo "  - comparison/ exists"

echo ""
echo "✓ Building E2E test package..."
cargo build

echo ""
echo "✓ Checking test compilation..."
cargo test --no-run

echo ""
echo "========================"
echo "✓ All checks passed!"
echo ""
echo "Next steps:"
echo "1. Build VaultGres Docker image: docker build -f docker/Dockerfile -t vaultgres:latest ."
echo "2. Run quick tests: ./run_all.sh quick"
echo "3. Start monitoring: ./run_all.sh monitor"
