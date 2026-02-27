#!/bin/bash

set -e

echo "=== Testing DDL Execution ==="
echo ""

./target/release/rustgres > server.log 2>&1 &
SERVER_PID=$!
sleep 2

cleanup() {
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "Test 1: CREATE TABLE"
psql -h localhost -p 5433 -U postgres -d postgres << EOF
CREATE TABLE users (id INT, name TEXT, email VARCHAR(100));
EOF
echo ""

echo "Test 2: Try to create duplicate table (should fail)"
psql -h localhost -p 5433 -U postgres -d postgres << EOF
CREATE TABLE users (id INT);
EOF
echo ""

echo "Test 3: CREATE another table"
psql -h localhost -p 5433 -U postgres -d postgres << EOF
CREATE TABLE products (id INT, name TEXT, price INT);
EOF
echo ""

echo "Test 4: DROP TABLE"
psql -h localhost -p 5433 -U postgres -d postgres << EOF
DROP TABLE products;
EOF
echo ""

echo "Test 5: DROP non-existent table (should fail)"
psql -h localhost -p 5433 -U postgres -d postgres << EOF
DROP TABLE products;
EOF
echo ""

echo "Test 6: DROP IF EXISTS (should succeed)"
psql -h localhost -p 5433 -U postgres -d postgres << EOF
DROP TABLE IF EXISTS products;
EOF
echo ""

echo "Test 7: Complete workflow"
psql -h localhost -p 5433 -U postgres -d postgres << EOF
CREATE TABLE test (id INT, data TEXT);
DROP TABLE test;
CREATE TABLE test (id INT, value INT);
DROP TABLE IF EXISTS test;
EOF
echo ""

echo "Server log:"
tail -30 server.log
