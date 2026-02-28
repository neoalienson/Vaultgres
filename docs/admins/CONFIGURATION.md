# Configuration Guide

Complete reference for RustGres configuration options.

## Configuration Files

### rustgres.conf

Main configuration file located at `$PGDATA/rustgres.conf`.

**Reload configuration without restart**:
```bash
rustgres reload -D /var/lib/rustgres/data
# Or
SELECT pg_reload_conf();
```

### pg_hba.conf

Client authentication configuration at `$PGDATA/pg_hba.conf`.

**Format**:
```
TYPE  DATABASE  USER  ADDRESS  METHOD  [OPTIONS]
```

### pg_ident.conf

User name mapping at `$PGDATA/pg_ident.conf`.

## Connection Settings

```ini
# Network
listen_addresses = 'localhost'    # '*' for all interfaces
port = 5432
max_connections = 100
superuser_reserved_connections = 3

# Socket
unix_socket_directories = '/tmp'
unix_socket_permissions = 0777

# SSL
ssl = off
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
ssl_ca_file = 'root.crt'
```

## Memory Settings

```ini
# Shared memory
shared_buffers = 256MB            # 25% of RAM (OLTP), 40% (OLAP)
huge_pages = try                  # on, off, try

# Per-query memory
work_mem = 4MB                    # Per sort/hash operation
maintenance_work_mem = 64MB       # VACUUM, CREATE INDEX
autovacuum_work_mem = -1          # -1 = use maintenance_work_mem

# Memory context
max_stack_depth = 2MB
dynamic_shared_memory_type = posix

# Cost-based vacuum delay
vacuum_cost_delay = 0             # 0 = disabled
vacuum_cost_page_hit = 1
vacuum_cost_page_miss = 10
vacuum_cost_page_dirty = 20
vacuum_cost_limit = 200
```

**Tuning Guidelines**:
- **shared_buffers**: Start with 25% of RAM, max 8GB
- **work_mem**: Total = work_mem × max_connections × 2-3
- **maintenance_work_mem**: 5-10% of RAM, max 1GB

## WAL Settings

```ini
# WAL behavior
wal_level = replica               # minimal, replica, logical
fsync = on                        # Never turn off in production
synchronous_commit = on           # on, remote_apply, remote_write, local, off
wal_sync_method = fdatasync       # fsync, fdatasync, open_sync, open_datasync

# WAL writing
wal_compression = on
wal_buffers = 16MB                # -1 = auto (1/32 of shared_buffers)
wal_writer_delay = 200ms
commit_delay = 0
commit_siblings = 5

# Checkpoints
checkpoint_timeout = 5min
checkpoint_completion_target = 0.9
checkpoint_warning = 30s
max_wal_size = 1GB
min_wal_size = 80MB

# Archiving
archive_mode = off
archive_command = ''
archive_timeout = 0
```

**Performance vs Durability**:
```ini
# Maximum durability (default)
fsync = on
synchronous_commit = on
wal_sync_method = fdatasync

# High performance (risk of data loss)
fsync = off                       # NEVER in production
synchronous_commit = off          # Can lose last few transactions
wal_sync_method = open_sync

# Balanced
fsync = on
synchronous_commit = local        # Don't wait for replica
wal_compression = on
```

## Query Tuning

```ini
# Planner cost constants
seq_page_cost = 1.0
random_page_cost = 4.0            # 1.1 for SSD, 4.0 for HDD
cpu_tuple_cost = 0.01
cpu_index_tuple_cost = 0.005
cpu_operator_cost = 0.0025
parallel_setup_cost = 1000.0
parallel_tuple_cost = 0.1

# Planner method configuration
enable_bitmapscan = on
enable_hashagg = on
enable_hashjoin = on
enable_indexscan = on
enable_indexonlyscan = on
enable_material = on
enable_mergejoin = on
enable_nestloop = on
enable_parallel_append = on
enable_parallel_hash = on
enable_partition_pruning = on
enable_partitionwise_join = off
enable_partitionwise_aggregate = off
enable_seqscan = on
enable_sort = on
enable_tidscan = on

# Genetic query optimizer
geqo = on
geqo_threshold = 12
geqo_effort = 5
geqo_pool_size = 0
geqo_generations = 0
geqo_selection_bias = 2.0
geqo_seed = 0.0

# Other planner options
default_statistics_target = 100   # 1-10000, higher = better estimates
constraint_exclusion = partition  # on, off, partition
cursor_tuple_fraction = 0.1
from_collapse_limit = 8
join_collapse_limit = 8
force_parallel_mode = off
jit = on
jit_above_cost = 100000
jit_inline_above_cost = 500000
jit_optimize_above_cost = 500000
```

## Parallel Query

```ini
max_worker_processes = 8
max_parallel_workers_per_gather = 2
max_parallel_maintenance_workers = 2
max_parallel_workers = 8
parallel_leader_participation = on
min_parallel_table_scan_size = 8MB
min_parallel_index_scan_size = 512kB
```

**Tuning**:
- Set `max_worker_processes` = number of CPU cores
- Set `max_parallel_workers` = max_worker_processes
- Set `max_parallel_workers_per_gather` = 2-4 for OLTP, 4-8 for OLAP

## Logging

```ini
# Where to log
log_destination = 'stderr'        # stderr, csvlog, syslog, eventlog
logging_collector = on
log_directory = 'log'
log_filename = 'rustgres-%Y-%m-%d_%H%M%S.log'
log_file_mode = 0600
log_rotation_age = 1d
log_rotation_size = 10MB
log_truncate_on_rotation = off

# When to log
log_min_messages = warning        # debug5-debug1, info, notice, warning, error, log, fatal, panic
log_min_error_statement = error
log_min_duration_statement = -1   # -1 = disabled, 0 = all, >0 = slow queries (ms)

# What to log
log_checkpoints = on
log_connections = off
log_disconnections = off
log_duration = off
log_error_verbosity = default     # terse, default, verbose
log_hostname = off
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_lock_waits = off
log_statement = 'none'            # none, ddl, mod, all
log_replication_commands = off
log_temp_files = -1
log_timezone = 'UTC'

# Process title
cluster_name = ''
update_process_title = on
```

**Recommended for Production**:
```ini
log_min_duration_statement = 1000  # Log queries > 1s
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
log_temp_files = 0                 # Log all temp files
log_autovacuum_min_duration = 0
```

## Autovacuum

```ini
autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = 1min
autovacuum_vacuum_threshold = 50
autovacuum_vacuum_scale_factor = 0.2
autovacuum_vacuum_insert_threshold = 1000
autovacuum_vacuum_insert_scale_factor = 0.2
autovacuum_analyze_threshold = 50
autovacuum_analyze_scale_factor = 0.1
autovacuum_freeze_max_age = 200000000
autovacuum_multixact_freeze_max_age = 400000000
autovacuum_vacuum_cost_delay = 2ms
autovacuum_vacuum_cost_limit = -1
```

**Aggressive Autovacuum** (for write-heavy workloads):
```ini
autovacuum_max_workers = 6
autovacuum_naptime = 10s
autovacuum_vacuum_scale_factor = 0.05
autovacuum_analyze_scale_factor = 0.02
```

## Replication

```ini
# Sending servers
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 0
wal_sender_timeout = 60s
track_commit_timestamp = off

# Primary server
synchronous_standby_names = ''
vacuum_defer_cleanup_age = 0

# Standby servers
hot_standby = on
max_standby_archive_delay = 30s
max_standby_streaming_delay = 30s
wal_receiver_create_temp_slot = off
wal_receiver_status_interval = 10s
hot_standby_feedback = off
wal_receiver_timeout = 60s
wal_retrieve_retry_interval = 5s
recovery_min_apply_delay = 0

# Subscribers
max_logical_replication_workers = 4
max_sync_workers_per_subscription = 2
```

## Lock Management

```ini
deadlock_timeout = 1s
max_locks_per_transaction = 64
max_pred_locks_per_transaction = 64
max_pred_locks_per_relation = -2
max_pred_locks_per_page = 2
```

## Statement Behavior

```ini
# Timeouts
statement_timeout = 0             # 0 = disabled
lock_timeout = 0
idle_in_transaction_session_timeout = 0
idle_session_timeout = 0

# Transaction
default_transaction_isolation = 'read committed'
default_transaction_read_only = off
default_transaction_deferrable = off

# Statement behavior
search_path = '"$user", public'
row_security = on
default_tablespace = ''
temp_tablespaces = ''
check_function_bodies = on
default_table_access_method = heap
session_replication_role = origin
```

## Client Connection Defaults

```ini
# Locale and formatting
datestyle = 'iso, mdy'
intervalstyle = 'postgres'
timezone = 'UTC'
timezone_abbreviations = 'Default'
extra_float_digits = 1
client_encoding = sql_ascii

# Shared library preloading
shared_preload_libraries = ''
local_preload_libraries = ''
session_preload_libraries = ''
jit_provider = 'llvmjit'
```

## Resource Limits

```ini
# Disk
temp_file_limit = -1              # -1 = unlimited

# Kernel resources
max_files_per_process = 1000
```

## Background Writer

```ini
bgwriter_delay = 200ms
bgwriter_lru_maxpages = 100
bgwriter_lru_multiplier = 2.0
bgwriter_flush_after = 512kB
```

## Asynchronous Behavior

```ini
effective_io_concurrency = 1      # 1-1000, set to # of drives in RAID
maintenance_io_concurrency = 10
max_worker_processes = 8
backend_flush_after = 0
old_snapshot_threshold = -1
```

## Performance Presets

### Small Server (2 CPU, 4GB RAM)

```ini
max_connections = 50
shared_buffers = 1GB
effective_cache_size = 3GB
maintenance_work_mem = 256MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 20MB
min_wal_size = 1GB
max_wal_size = 4GB
max_worker_processes = 2
max_parallel_workers_per_gather = 1
max_parallel_workers = 2
```

### Medium Server (8 CPU, 32GB RAM)

```ini
max_connections = 200
shared_buffers = 8GB
effective_cache_size = 24GB
maintenance_work_mem = 2GB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 40MB
min_wal_size = 2GB
max_wal_size = 8GB
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
```

### Large Server (32 CPU, 128GB RAM)

```ini
max_connections = 500
shared_buffers = 32GB
effective_cache_size = 96GB
maintenance_work_mem = 4GB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 500
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 64MB
min_wal_size = 4GB
max_wal_size = 16GB
max_worker_processes = 32
max_parallel_workers_per_gather = 8
max_parallel_workers = 32
```

## Workload-Specific Tuning

### OLTP (High Concurrency, Small Transactions)

```ini
shared_buffers = 25% of RAM
work_mem = 4MB
maintenance_work_mem = 512MB
random_page_cost = 1.1
effective_io_concurrency = 200
max_connections = 500
checkpoint_timeout = 5min
checkpoint_completion_target = 0.9
```

### OLAP (Low Concurrency, Large Queries)

```ini
shared_buffers = 40% of RAM
work_mem = 256MB
maintenance_work_mem = 2GB
random_page_cost = 1.1
effective_io_concurrency = 200
max_connections = 50
max_parallel_workers_per_gather = 8
checkpoint_timeout = 15min
checkpoint_completion_target = 0.9
```

### Mixed Workload

```ini
shared_buffers = 32% of RAM
work_mem = 32MB
maintenance_work_mem = 1GB
random_page_cost = 1.1
effective_io_concurrency = 200
max_connections = 200
max_parallel_workers_per_gather = 4
checkpoint_timeout = 10min
checkpoint_completion_target = 0.9
```

## Configuration Tools

### pg_tune

```bash
# Generate optimized config
rustgres-tune --db-type=oltp --total-memory=32GB --cpus=8 > rustgres.conf.new
```

### View Current Settings

```sql
-- All settings
SELECT name, setting, unit, context FROM pg_settings ORDER BY name;

-- Modified settings
SELECT name, setting, source FROM pg_settings WHERE source != 'default';

-- Pending restart
SELECT name, setting, pending_restart FROM pg_settings WHERE pending_restart;
```

### Change Settings

```sql
-- Session level
SET work_mem = '256MB';

-- Transaction level
BEGIN;
SET LOCAL work_mem = '256MB';
-- ...
COMMIT;

-- Persistent (requires reload)
ALTER SYSTEM SET shared_buffers = '8GB';
SELECT pg_reload_conf();

-- User/database level
ALTER USER myuser SET work_mem = '128MB';
ALTER DATABASE mydb SET random_page_cost = 1.1;
```

## Monitoring Configuration

```sql
-- Check buffer hit ratio
SELECT 
    sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) AS cache_hit_ratio
FROM pg_statio_user_tables;

-- Check connection usage
SELECT count(*) * 100.0 / current_setting('max_connections')::int AS pct_used
FROM pg_stat_activity;

-- Check checkpoint frequency
SELECT 
    checkpoints_timed,
    checkpoints_req,
    checkpoint_write_time,
    checkpoint_sync_time
FROM pg_stat_bgwriter;
```

## Best Practices

1. **Start conservative**: Use defaults, then tune based on monitoring
2. **Test changes**: Apply to staging first
3. **Monitor impact**: Watch metrics before/after changes
4. **Document changes**: Keep track of what you changed and why
5. **Use version control**: Store configs in git
6. **Automate**: Use configuration management (Ansible, Puppet)
