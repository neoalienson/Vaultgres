# Quick Start Tutorial

Get started with RustGres in 5 minutes.

## Installation

```bash
# Download and install
curl -L https://github.com/rustgres/rustgres/releases/latest/download/rustgres-linux-x64.tar.gz | tar xz
sudo mv rustgres /usr/local/bin/

# Verify installation
rustgres --version
```

## Initialize Database

```bash
# Create data directory
mkdir -p ~/rustgres-data

# Initialize database cluster
rustgres init -D ~/rustgres-data

# Start server
rustgres start -D ~/rustgres-data
```

## Connect to Database

```bash
# Using psql (PostgreSQL client)
psql -h localhost -p 5432 -U postgres

# Or using any PostgreSQL-compatible client
```

## Basic Operations

### Create a Table

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Insert Data

```sql
-- Single row
INSERT INTO users (name, email, age) 
VALUES ('Alice', 'alice@example.com', 30);

-- Multiple rows
INSERT INTO users (name, email, age) VALUES 
    ('Bob', 'bob@example.com', 25),
    ('Charlie', 'charlie@example.com', 35),
    ('Diana', 'diana@example.com', 28);
```

### Query Data

```sql
-- Select all
SELECT * FROM users;

-- Filter
SELECT name, email FROM users WHERE age > 25;

-- Order by
SELECT * FROM users ORDER BY age DESC;

-- Limit
SELECT * FROM users LIMIT 2;

-- Aggregate
SELECT AVG(age) as avg_age, COUNT(*) as total FROM users;
```

### Update Data

```sql
UPDATE users SET age = 31 WHERE name = 'Alice';
```

### Delete Data

```sql
DELETE FROM users WHERE age < 26;
```

## Indexes

```sql
-- Create index for faster queries
CREATE INDEX idx_users_email ON users(email);

-- Query using index
SELECT * FROM users WHERE email = 'alice@example.com';

-- Check query plan
EXPLAIN SELECT * FROM users WHERE email = 'alice@example.com';
```

## Transactions

```sql
-- Start transaction
BEGIN;

-- Make changes
INSERT INTO users (name, email, age) VALUES ('Eve', 'eve@example.com', 29);
UPDATE users SET age = age + 1 WHERE name = 'Alice';

-- Commit changes
COMMIT;

-- Or rollback
-- ROLLBACK;
```

## Joins

```sql
-- Create orders table
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert orders
INSERT INTO orders (user_id, amount) VALUES 
    (1, 99.99),
    (1, 149.99),
    (2, 79.99);

-- Join tables
SELECT 
    u.name,
    u.email,
    o.amount,
    o.created_at
FROM users u
JOIN orders o ON u.id = o.user_id
ORDER BY o.created_at DESC;

-- Aggregate with join
SELECT 
    u.name,
    COUNT(o.id) as order_count,
    SUM(o.amount) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name
ORDER BY total_spent DESC;
```

## Advanced Features

### Window Functions

```sql
SELECT 
    name,
    age,
    AVG(age) OVER () as avg_age,
    RANK() OVER (ORDER BY age DESC) as age_rank
FROM users;
```

### Common Table Expressions (CTEs)

```sql
WITH high_spenders AS (
    SELECT 
        user_id,
        SUM(amount) as total
    FROM orders
    GROUP BY user_id
    HAVING SUM(amount) > 100
)
SELECT u.name, hs.total
FROM users u
JOIN high_spenders hs ON u.id = hs.user_id;
```

### JSON Data

```sql
-- Create table with JSON column
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_type TEXT,
    data JSONB
);

-- Insert JSON data
INSERT INTO events (event_type, data) VALUES 
    ('login', '{"user_id": 1, "ip": "192.168.1.1"}'),
    ('purchase', '{"user_id": 1, "amount": 99.99, "items": ["book", "pen"]}');

-- Query JSON data
SELECT 
    event_type,
    data->>'user_id' as user_id,
    data->>'amount' as amount
FROM events
WHERE data->>'user_id' = '1';

-- JSON operators
SELECT * FROM events WHERE data @> '{"user_id": 1}';
SELECT * FROM events WHERE data ? 'amount';
```

## Performance Tips

### Use EXPLAIN

```sql
-- See query execution plan
EXPLAIN SELECT * FROM users WHERE age > 25;

-- See actual execution statistics
EXPLAIN ANALYZE SELECT * FROM users WHERE age > 25;
```

### Create Appropriate Indexes

```sql
-- Index for WHERE clauses
CREATE INDEX idx_users_age ON users(age);

-- Partial index for specific conditions
CREATE INDEX idx_active_users ON users(email) WHERE age > 18;

-- Multi-column index for combined queries
CREATE INDEX idx_users_age_name ON users(age, name);
```

### Update Statistics

```sql
-- Analyze table for better query plans
ANALYZE users;

-- Analyze all tables
ANALYZE;
```

### Use Prepared Statements

```python
# Python example with psycopg2
import psycopg2

conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
cur = conn.cursor()

# Prepared statement (automatically cached)
cur.execute("SELECT * FROM users WHERE email = %s", ("alice@example.com",))
result = cur.fetchall()
```

## Backup and Restore

### Backup

```bash
# Dump database
rustgres pg_dump mydb > mydb_backup.sql

# Dump specific table
rustgres pg_dump -t users mydb > users_backup.sql

# Binary format (faster)
rustgres pg_dump -Fc mydb > mydb_backup.dump
```

### Restore

```bash
# Restore from SQL dump
rustgres psql mydb < mydb_backup.sql

# Restore from binary dump
rustgres pg_restore -d mydb mydb_backup.dump
```

## Monitoring

### Check Server Status

```sql
-- Active connections
SELECT * FROM pg_stat_activity;

-- Database size
SELECT 
    pg_database.datname,
    pg_size_pretty(pg_database_size(pg_database.datname)) AS size
FROM pg_database;

-- Table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;

-- Index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```

### Performance Metrics

```sql
-- Cache hit ratio (should be > 99%)
SELECT 
    sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) * 100 AS cache_hit_ratio
FROM pg_statio_user_tables;

-- Slow queries (if log_min_duration_statement is set)
-- Check log files in data/log/
```

## Configuration

### Basic Tuning

Edit `~/rustgres-data/rustgres.conf`:

```ini
# Memory (adjust based on available RAM)
shared_buffers = 256MB          # 25% of RAM
work_mem = 4MB
maintenance_work_mem = 64MB

# Connections
max_connections = 100

# Logging
log_min_duration_statement = 1000  # Log queries > 1s
log_line_prefix = '%t [%p]: '
```

Reload configuration:
```bash
rustgres reload -D ~/rustgres-data
```

## Stop Server

```bash
# Graceful shutdown
rustgres stop -D ~/rustgres-data

# Fast shutdown
rustgres stop -D ~/rustgres-data -m fast

# Immediate shutdown (not recommended)
rustgres stop -D ~/rustgres-data -m immediate
```

## Next Steps

- Read the [SQL Reference](SQL.md) for complete SQL syntax
- Learn about [Configuration](CONFIGURATION.md) options
- Explore [Architecture](ARCHITECTURE.md) to understand internals
- Check the [Roadmap](ROADMAP.md) for upcoming features
- Join our [Discord](https://discord.gg/rustgres) community

## Common Issues

### Port Already in Use

```bash
# Change port in rustgres.conf
port = 5433

# Or specify when starting
rustgres start -D ~/rustgres-data -o "-p 5433"
```

### Connection Refused

```bash
# Check if server is running
rustgres status -D ~/rustgres-data

# Check listen address in rustgres.conf
listen_addresses = '*'  # Allow all connections

# Update pg_hba.conf to allow connections
host    all    all    0.0.0.0/0    scram-sha-256
```

### Out of Memory

```bash
# Reduce memory settings in rustgres.conf
shared_buffers = 128MB
work_mem = 2MB
```

## Getting Help

- **Documentation**: https://rustgres.org/docs
- **Discord**: https://discord.gg/rustgres
- **GitHub Issues**: https://github.com/rustgres/rustgres/issues
- **Stack Overflow**: Tag questions with `rustgres`

Happy querying! 🚀
