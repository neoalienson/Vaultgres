#!/bin/bash

set -e

echo "=== Testing DROP TABLE Support ==="
echo ""

./target/release/rustgres > server.log 2>&1 &
SERVER_PID=$!
sleep 2

cleanup() {
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "Test 1: DROP TABLE"
echo "DROP TABLE users;" | psql -h localhost -p 5433 -U postgres -d postgres 2>&1 | head -5
echo ""

echo "Test 2: DROP TABLE IF EXISTS"
echo "DROP TABLE IF EXISTS products;" | psql -h localhost -p 5433 -U postgres -d postgres 2>&1 | head -5
echo ""

echo "Test 3: Multiple DROP statements"
psql -h localhost -p 5433 -U postgres -d postgres << EOF 2>&1 | head -10
DROP TABLE orders;
DROP TABLE IF EXISTS items;
DROP TABLE customers;
EOF
echo ""

echo "Test 4: Complete DDL workflow"
psql -h localhost -p 5433 -U postgres -d postgres << EOF 2>&1 | head -15
CREATE TABLE test_table (id INT, name TEXT);
DESCRIBE test_table;
DROP TABLE test_table;
DROP TABLE IF EXISTS test_table;
EOF
echo ""

echo "Server log:"
tail -20 server.log
