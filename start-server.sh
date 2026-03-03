#!/bin/bash

# VaultGres Server Startup Script

echo "🦀 VaultGres v0.1.0 - PostgreSQL-compatible RDBMS"
echo "================================================"
echo ""

# Build if needed
if [ ! -f "target/release/vaultgres" ]; then
    echo "📦 Building VaultGres..."
    cargo build --release
    echo ""
fi

# Start server
echo "🚀 Starting server..."
./target/release/vaultgres
