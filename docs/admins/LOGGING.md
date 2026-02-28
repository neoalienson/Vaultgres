# RustGres Logging Configuration

## Environment Variables

### RUSTGRES_LOG_LEVEL
Sets the logging level. Default: `info`

**Levels:**
- `error` - Only errors
- `warn` - Warnings and errors
- `info` - Informational messages (default)
- `debug` - Debug information
- `trace` - Verbose tracing

### RUSTGRES_LOG_SCOPE
Sets which modules to log. Default: `*` (all modules)

**Scopes:**
- `*` - All modules (default)
- `protocol` - Protocol layer only
- `parser` - SQL parser only
- `executor` - Query executor only
- `storage` - Storage layer only
- `transaction` - Transaction manager only
- `wal` - Write-ahead log only

**Multiple scopes:** Comma-separated list
```bash
RUSTGRES_LOG_SCOPE=protocol,parser
```

## Usage Examples

### Default (info level, all modules)
```bash
./target/release/rustgres
```

### Debug level for all modules
```bash
RUSTGRES_LOG_LEVEL=debug ./target/release/rustgres
```

### Info level for protocol only
```bash
RUSTGRES_LOG_SCOPE=protocol ./target/release/rustgres
```

### Debug level for protocol and parser
```bash
RUSTGRES_LOG_LEVEL=debug RUSTGRES_LOG_SCOPE=protocol,parser ./target/release/rustgres
```

### Trace level for storage
```bash
RUSTGRES_LOG_LEVEL=trace RUSTGRES_LOG_SCOPE=storage ./target/release/rustgres
```

### Error level only
```bash
RUSTGRES_LOG_LEVEL=error ./target/release/rustgres
```

## Log Output

Logs are written to stderr with format:
```
[2024-02-27T22:30:00Z INFO  rustgres::protocol] Query: SELECT 1;
[2024-02-27T22:30:00Z DEBUG rustgres::parser] Parsed statement: Select(...)
```

## Production Recommendations

**Production:**
```bash
RUSTGRES_LOG_LEVEL=warn ./target/release/rustgres
```

**Development:**
```bash
RUSTGRES_LOG_LEVEL=debug ./target/release/rustgres
```

**Troubleshooting:**
```bash
RUSTGRES_LOG_LEVEL=trace RUSTGRES_LOG_SCOPE=protocol,parser ./target/release/rustgres
```
