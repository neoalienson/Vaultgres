#!/bin/bash

set -e

echo "=== Testing CREATE TABLE Support ==="
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
echo "CREATE TABLE users (id INT, name TEXT);" | psql -h localhost -p 5433 -U postgres -d postgres 2>&1 | head -5
echo ""

echo "Test 2: CREATE TABLE with VARCHAR"
echo "CREATE TABLE products (id INT, name VARCHAR(100), price INT);" | psql -h localhost -p 5433 -U postgres -d postgres 2>&1 | head -5
echo ""

echo "Test 3: Multiple CREATE TABLEs"
psql -h localhost -p 5433 -U postgres -d postgres << EOF 2>&1 | head -10
CREATE TABLE orders (id INT, user_id INT);
CREATE TABLE items (id INT, order_id INT, product_id INT);
EOF
echo ""

echo "Server log:"
tail -15 server.log
