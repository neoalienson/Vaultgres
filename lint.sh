#!/bin/bash
# Lint script for RustGres

set -e

echo "Running Clippy linter..."
cargo clippy --all-targets --all-features

echo ""
echo "Clippy check completed!"
echo ""
echo "To fix warnings automatically (where possible):"
echo "  cargo clippy --fix --all-targets --all-features"
echo ""
echo "To deny all warnings (fail on warnings):"
echo "  cargo clippy --all-targets --all-features -- -D warnings"
