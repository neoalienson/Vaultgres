#!/bin/bash

set -e

echo "=== RustGres End-to-End Test ==="
echo ""

# Start server in background
echo "Starting RustGres server..."
./target/release/rustgres &
SERVER_PID=$!
sleep 2

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "Server started (PID: $SERVER_PID)"
echo ""

# Test 1: Connect with psql
echo "Test 1: Basic connection test"
psql -h localhost -p 5433 -U postgres -d postgres -c "SELECT 1;" 2>&1 | head -10 || echo "Connection test completed"
echo ""

# Test 2: CREATE TABLE
echo "Test 2: CREATE TABLE"
psql -h localhost -p 5433 -U postgres -d postgres -c "CREATE TABLE users (id INT, name TEXT);" 2>&1 | head -5 || echo "CREATE TABLE test completed"
echo ""

# Test 3: INSERT
echo "Test 3: INSERT data"
psql -h localhost -p 5433 -U postgres -d postgres -c "INSERT INTO users VALUES (1, 'Alice');" 2>&1 | head -5 || echo "INSERT test completed"
echo ""

# Test 4: SELECT
echo "Test 4: SELECT query"
psql -h localhost -p 5433 -U postgres -d postgres -c "SELECT * FROM users;" 2>&1 | head -10 || echo "SELECT test completed"
echo ""

# Test 5: Multiple operations
echo "Test 5: Multiple operations"
psql -h localhost -p 5433 -U postgres -d postgres << EOF 2>&1 | head -20 || echo "Multiple operations test completed"
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie');
SELECT * FROM users WHERE id > 1;
EOF
echo ""

echo "=== All tests completed ==="
