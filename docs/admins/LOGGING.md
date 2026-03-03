# VaultGres Logging Configuration

## Environment Variables

### VAULTGRES_LOG_LEVEL
Sets the logging level. Default: `info`

**Levels:**
- `error` - Only errors
- `warn` - Warnings and errors
- `info` - Informational messages (default)
- `debug` - Debug information
- `trace` - Verbose tracing

### VAULTGRES_LOG_SCOPE
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
VAULTGRES_LOG_SCOPE=protocol,parser
```

## Usage Examples

### Default (info level, all modules)
```bash
./target/release/vaultgres
```

### Debug level for all modules
```bash
VAULTGRES_LOG_LEVEL=debug ./target/release/vaultgres
```

### Info level for protocol only
```bash
VAULTGRES_LOG_SCOPE=protocol ./target/release/vaultgres
```

### Debug level for protocol and parser
```bash
VAULTGRES_LOG_LEVEL=debug VAULTGRES_LOG_SCOPE=protocol,parser ./target/release/vaultgres
```

### Trace level for storage
```bash
VAULTGRES_LOG_LEVEL=trace VAULTGRES_LOG_SCOPE=storage ./target/release/vaultgres
```

### Error level only
```bash
VAULTGRES_LOG_LEVEL=error ./target/release/vaultgres
```

## Log Output

Logs are written to stderr with format:
```
[2024-02-27T22:30:00Z INFO  vaultgres::protocol] Query: SELECT 1;
[2024-02-27T22:30:00Z DEBUG vaultgres::parser] Parsed statement: Select(...)
```

## Production Recommendations

**Production:**
```bash
VAULTGRES_LOG_LEVEL=warn ./target/release/vaultgres
```

**Development:**
```bash
VAULTGRES_LOG_LEVEL=debug ./target/release/vaultgres
```

**Troubleshooting:**
```bash
VAULTGRES_LOG_LEVEL=trace VAULTGRES_LOG_SCOPE=protocol,parser ./target/release/vaultgres
```
