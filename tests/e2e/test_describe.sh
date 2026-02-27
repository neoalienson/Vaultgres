#!/bin/bash

set -e

echo "=== Testing DESCRIBE/DESC Support ==="
echo ""

./target/release/rustgres > server.log 2>&1 &
SERVER_PID=$!
sleep 2

cleanup() {
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "Test 1: DESCRIBE table"
echo "DESCRIBE users;" | psql -h localhost -p 5433 -U postgres -d postgres 2>&1 | head -5
echo ""

echo "Test 2: DESC table (short form)"
echo "DESC products;" | psql -h localhost -p 5433 -U postgres -d postgres 2>&1 | head -5
echo ""

echo "Test 3: Multiple DESCRIBE statements"
psql -h localhost -p 5433 -U postgres -d postgres << EOF 2>&1 | head -10
DESCRIBE orders;
DESC items;
DESCRIBE customers;
EOF
echo ""

echo "Server log:"
tail -15 server.log
