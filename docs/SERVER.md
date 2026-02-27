# RustGres Server

## Quick Start

### Start the Server

```bash
# Option 1: Using the startup script
./start-server.sh

# Option 2: Direct execution
cargo run --release

# Option 3: Run the binary directly
./target/release/rustgres
```

The server will start on `127.0.0.1:5433` by default.

### Connect with psql

```bash
# Connect to RustGres
psql -h 127.0.0.1 -p 5433 -U postgres -d testdb

# Run queries
SELECT * FROM users;
INSERT INTO users VALUES (1, 'Alice');
```

### Test the Server

```bash
# In another terminal, test with psql
psql -h 127.0.0.1 -p 5433 -U postgres -d testdb -c "SELECT 1"
```

## Server Output

When started, you'll see:
```
🚀 RustGres v0.1.0 starting...
📡 Listening on 127.0.0.1:5433
✅ Ready for connections

Connect with: psql -h 127.0.0.1 -p 5433 -U postgres -d testdb
```

## Supported Features (v0.1.0)

- ✅ PostgreSQL wire protocol
- ✅ Basic authentication (no password)
- ✅ SQL parsing (SELECT/INSERT/UPDATE/DELETE)
- ✅ Query execution
- ✅ MVCC transactions
- ✅ WAL and recovery

## Limitations

- No persistent storage (in-memory only)
- No password authentication
- No SSL/TLS
- No prepared statements
- Limited SQL features

## Development

```bash
# Run tests
cargo test

# Build debug version
cargo build

# Build release version
cargo build --release

# Run with logging
RUST_LOG=debug cargo run
```

## Architecture

```
Client (psql) → TCP Connection → Protocol Handler → Parser → Executor → Storage
```

## Troubleshooting

**Port already in use:**
```bash
# Check what's using port 5433
lsof -i :5433

# Kill the process
kill -9 <PID>
```

**Connection refused:**
- Ensure the server is running
- Check firewall settings
- Verify the port number

**Parse errors:**
- RustGres supports basic SQL only
- Check the SQL syntax matches supported features

## Next Steps

See [RELEASE_v0.1.0.md](RELEASE_v0.1.0.md) for full feature list and roadmap.
