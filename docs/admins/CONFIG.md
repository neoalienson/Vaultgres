# VaultGres Configuration Guide

## Configuration File

VaultGres uses YAML for configuration. By default, it looks for `config.yaml` in the current directory.

### Specify Custom Config

```bash
# Using environment variable
VAULTGRES_CONFIG=config.prod.yaml ./target/release/vaultgres

# Or copy your config to default location
cp config.prod.yaml config.yaml
./target/release/vaultgres
```

## Configuration Sections

### Server

```yaml
server:
  host: "127.0.0.1"      # Host to bind to
  port: 5433              # Port to listen on
  max_connections: 100    # Maximum concurrent connections
```

**Options:**
- `host`: IP address to bind (use "0.0.0.0" for all interfaces)
- `port`: TCP port (default: 5433, PostgreSQL uses 5432)
- `max_connections`: Maximum number of concurrent client connections

### Storage

```yaml
storage:
  data_dir: "./data"           # Directory for data files
  wal_dir: "./wal"             # Directory for WAL files
  buffer_pool_size: 1000       # Number of pages in buffer pool
  page_size: 8192              # Page size in bytes
```

**Options:**
- `data_dir`: Path to store data files (created if doesn't exist)
- `wal_dir`: Path to store WAL files (created if doesn't exist)
- `buffer_pool_size`: Number of 8KB pages to cache in memory
- `page_size`: Size of each page (default: 8192 bytes)

**Memory Usage:**
```
Total memory = buffer_pool_size × page_size
Example: 1000 × 8192 = 8 MB
```

### Logging

```yaml
logging:
  level: "info"              # Log level
  scope: "*"                 # Modules to log
  file: "./logs/server.log"  # Optional log file
```

**Log Levels:**
- `error` - Only errors
- `warn` - Warnings and errors
- `info` - Informational messages (recommended)
- `debug` - Debug information
- `trace` - Verbose tracing

**Log Scopes:**
- `*` - All modules (default)
- `protocol` - Protocol layer only
- `parser,executor` - Multiple modules (comma-separated)

**Log File:**
- If not specified, logs go to stderr
- If specified, logs are written to file

### Transaction

```yaml
transaction:
  timeout: 300           # Transaction timeout in seconds
  mvcc_enabled: true     # Enable MVCC
```

**Options:**
- `timeout`: Seconds before transaction times out
- `mvcc_enabled`: Enable Multi-Version Concurrency Control

### WAL (Write-Ahead Log)

```yaml
wal:
  segment_size: 16        # WAL segment size in MB
  compression: false      # Enable WAL compression
  sync_on_commit: true    # Sync WAL to disk on commit
```

**Options:**
- `segment_size`: Size of each WAL segment file in MB
- `compression`: Compress WAL records (not implemented yet)
- `sync_on_commit`: Force sync to disk on transaction commit

### Performance

```yaml
performance:
  worker_threads: 4      # Number of worker threads
  query_cache: false     # Enable query result caching
```

**Options:**
- `worker_threads`: Number of threads for parallel operations (not used yet)
- `query_cache`: Cache query results (not implemented yet)

## Example Configurations

### Development

```yaml
server:
  host: "127.0.0.1"
  port: 5433
  max_connections: 50

storage:
  data_dir: "./data"
  wal_dir: "./wal"
  buffer_pool_size: 500

logging:
  level: "debug"
  scope: "*"
```

### Production

```yaml
server:
  host: "0.0.0.0"
  port: 5432
  max_connections: 200

storage:
  data_dir: "/var/lib/vaultgres/data"
  wal_dir: "/var/lib/vaultgres/wal"
  buffer_pool_size: 10000

logging:
  level: "warn"
  scope: "*"
  file: "/var/log/vaultgres/vaultgres.log"

wal:
  sync_on_commit: true
```

### Testing

```yaml
server:
  host: "127.0.0.1"
  port: 5434
  max_connections: 10

storage:
  data_dir: "./test_data"
  wal_dir: "./test_wal"
  buffer_pool_size: 100

logging:
  level: "trace"
  scope: "protocol,parser"
```

## Environment Variable Override

Environment variables take precedence over config file:

```bash
# Override config file settings
VAULTGRES_CONFIG=config.prod.yaml \
VAULTGRES_LOG_LEVEL=debug \
VAULTGRES_LOG_SCOPE=protocol \
./target/release/vaultgres
```

## Validation

VaultGres validates the configuration on startup:
- Creates directories if they don't exist
- Checks port availability
- Validates numeric ranges

Invalid configurations will show an error and use defaults.

## Default Configuration

If no config file is found, VaultGres uses these defaults:
- Host: 127.0.0.1
- Port: 5433
- Data dir: ./data
- WAL dir: ./wal
- Buffer pool: 1000 pages (8 MB)
- Log level: info
- Log scope: * (all)
