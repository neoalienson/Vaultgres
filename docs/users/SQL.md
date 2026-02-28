# SQL Reference

RustGres implements SQL:2016 standard with PostgreSQL compatibility.

## Data Definition Language (DDL)

### CREATE TABLE

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name TEXT,
    age INTEGER CHECK (age >= 0),
    balance DECIMAL(10, 2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB
);

-- With storage options
CREATE TABLE events (
    id BIGSERIAL,
    data JSONB
) WITH (
    storage_engine = 'columnar',
    compression = 'lz4'
);

-- Partitioned table
CREATE TABLE measurements (
    time TIMESTAMP NOT NULL,
    value DOUBLE PRECISION
) PARTITION BY RANGE (time);

CREATE TABLE measurements_2024_01 PARTITION OF measurements
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

### CREATE INDEX

```sql
-- B-Tree index (default)
CREATE INDEX idx_users_email ON users(email);

-- Unique index
CREATE UNIQUE INDEX idx_users_email_unique ON users(email);

-- Partial index
CREATE INDEX idx_active_users ON users(email) WHERE active = true;

-- Expression index
CREATE INDEX idx_users_lower_email ON users(LOWER(email));

-- Multi-column index
CREATE INDEX idx_users_name_age ON users(name, age);

-- Concurrent index creation
CREATE INDEX CONCURRENTLY idx_users_created ON users(created_at);

-- Other index types
CREATE INDEX idx_users_name_gin ON users USING GIN(name gin_trgm_ops);
CREATE INDEX idx_location_gist ON places USING GIST(location);
CREATE INDEX idx_tags_hash ON posts USING HASH(tags);
CREATE INDEX idx_time_brin ON events USING BRIN(timestamp);
```

### ALTER TABLE

```sql
-- Add column
ALTER TABLE users ADD COLUMN phone VARCHAR(20);

-- Drop column
ALTER TABLE users DROP COLUMN phone;

-- Rename column
ALTER TABLE users RENAME COLUMN name TO full_name;

-- Change column type
ALTER TABLE users ALTER COLUMN age TYPE BIGINT;

-- Add constraint
ALTER TABLE users ADD CONSTRAINT check_age CHECK (age >= 18);

-- Drop constraint
ALTER TABLE users DROP CONSTRAINT check_age;

-- Add foreign key
ALTER TABLE orders ADD CONSTRAINT fk_user 
    FOREIGN KEY (user_id) REFERENCES users(id);
```

### DROP

```sql
DROP TABLE users;
DROP TABLE IF EXISTS users CASCADE;
DROP INDEX idx_users_email;
DROP DATABASE mydb;
```

## Data Manipulation Language (DML)

### INSERT

```sql
-- Single row
INSERT INTO users (email, name) VALUES ('user@example.com', 'John');

-- Multiple rows
INSERT INTO users (email, name) VALUES 
    ('user1@example.com', 'Alice'),
    ('user2@example.com', 'Bob');

-- From SELECT
INSERT INTO users_backup SELECT * FROM users;

-- RETURNING clause
INSERT INTO users (email) VALUES ('new@example.com') RETURNING id;

-- ON CONFLICT (upsert)
INSERT INTO users (id, email, name) VALUES (1, 'user@example.com', 'John')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

INSERT INTO users (email) VALUES ('user@example.com')
ON CONFLICT (email) DO NOTHING;
```

### SELECT

```sql
-- Basic query
SELECT id, email FROM users WHERE age > 18;

-- Joins
SELECT u.name, o.total 
FROM users u
JOIN orders o ON u.id = o.user_id;

-- Aggregation
SELECT country, COUNT(*), AVG(age)
FROM users
GROUP BY country
HAVING COUNT(*) > 10;

-- Window functions
SELECT 
    name,
    salary,
    AVG(salary) OVER (PARTITION BY department) as dept_avg,
    RANK() OVER (ORDER BY salary DESC) as salary_rank
FROM employees;

-- CTEs
WITH active_users AS (
    SELECT * FROM users WHERE last_login > NOW() - INTERVAL '30 days'
)
SELECT * FROM active_users WHERE age > 18;

-- Recursive CTE
WITH RECURSIVE subordinates AS (
    SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id 
    FROM employees e
    JOIN subordinates s ON e.manager_id = s.id
)
SELECT * FROM subordinates;

-- LATERAL join
SELECT u.name, o.* 
FROM users u
CROSS JOIN LATERAL (
    SELECT * FROM orders WHERE user_id = u.id ORDER BY created_at DESC LIMIT 5
) o;
```

### UPDATE

```sql
-- Simple update
UPDATE users SET age = 30 WHERE id = 1;

-- Multiple columns
UPDATE users SET age = 30, name = 'John' WHERE id = 1;

-- From another table
UPDATE users u SET balance = o.total
FROM orders o
WHERE u.id = o.user_id;

-- RETURNING clause
UPDATE users SET age = age + 1 WHERE id = 1 RETURNING age;
```

### DELETE

```sql
-- Simple delete
DELETE FROM users WHERE id = 1;

-- With subquery
DELETE FROM users WHERE id IN (SELECT user_id FROM banned_users);

-- RETURNING clause
DELETE FROM users WHERE age < 18 RETURNING id, email;

-- TRUNCATE (faster)
TRUNCATE TABLE users;
TRUNCATE TABLE users RESTART IDENTITY CASCADE;
```

## Data Types

### Numeric Types

```sql
SMALLINT        -- 2 bytes, -32768 to 32767
INTEGER, INT    -- 4 bytes, -2^31 to 2^31-1
BIGINT          -- 8 bytes, -2^63 to 2^63-1
SERIAL          -- Auto-incrementing INTEGER
BIGSERIAL       -- Auto-incrementing BIGINT

DECIMAL(p, s)   -- Exact numeric, p digits, s decimal places
NUMERIC(p, s)   -- Same as DECIMAL
REAL            -- 4 bytes, 6 decimal digits precision
DOUBLE PRECISION -- 8 bytes, 15 decimal digits precision
```

### String Types

```sql
CHAR(n)         -- Fixed-length, blank-padded
VARCHAR(n)      -- Variable-length with limit
TEXT            -- Variable unlimited length
```

### Date/Time Types

```sql
DATE            -- Date (year, month, day)
TIME            -- Time of day
TIMESTAMP       -- Date and time
TIMESTAMPTZ     -- Timestamp with timezone
INTERVAL        -- Time interval
```

### Boolean

```sql
BOOLEAN         -- true, false, NULL
```

### Binary

```sql
BYTEA           -- Binary data
```

### JSON

```sql
JSON            -- JSON data (text storage)
JSONB           -- JSON data (binary storage, indexed)
```

### Arrays

```sql
INTEGER[]       -- Array of integers
TEXT[]          -- Array of text
```

### UUID

```sql
UUID            -- Universally unique identifier
```

### Geometric

```sql
POINT           -- Point (x, y)
LINE            -- Infinite line
LSEG            -- Line segment
BOX             -- Rectangle
PATH            -- Geometric path
POLYGON         -- Polygon
CIRCLE          -- Circle
```

## Operators

### Comparison

```sql
=, <>, !=       -- Equal, not equal
<, <=, >, >=    -- Less than, greater than
BETWEEN         -- Range check
IN              -- Set membership
IS NULL         -- NULL check
IS DISTINCT FROM -- NULL-safe comparison
```

### Logical

```sql
AND, OR, NOT
```

### Pattern Matching

```sql
LIKE            -- Pattern matching with % and _
ILIKE           -- Case-insensitive LIKE
SIMILAR TO      -- SQL regex
~, ~*           -- POSIX regex (case-sensitive, insensitive)
```

### Array

```sql
@>              -- Contains
<@              -- Contained by
&&              -- Overlap
||              -- Concatenation
```

### JSON

```sql
->              -- Get JSON object field
->>             -- Get JSON object field as text
#>              -- Get JSON object at path
#>>             -- Get JSON object at path as text
@>              -- Contains
<@              -- Contained by
?               -- Key exists
?|              -- Any key exists
?&              -- All keys exist
```

## Functions

### Aggregate Functions

```sql
COUNT(*)        -- Count rows
SUM(column)     -- Sum values
AVG(column)     -- Average
MIN(column)     -- Minimum
MAX(column)     -- Maximum
ARRAY_AGG(column) -- Aggregate to array
STRING_AGG(column, delimiter) -- Concatenate strings
JSON_AGG(column) -- Aggregate to JSON array
```

### Window Functions

```sql
ROW_NUMBER()    -- Sequential number
RANK()          -- Rank with gaps
DENSE_RANK()    -- Rank without gaps
NTILE(n)        -- Divide into n buckets
LAG(column, offset) -- Previous row value
LEAD(column, offset) -- Next row value
FIRST_VALUE(column) -- First value in window
LAST_VALUE(column) -- Last value in window
```

### String Functions

```sql
CONCAT(str1, str2, ...) -- Concatenate
LENGTH(str)     -- String length
LOWER(str)      -- Convert to lowercase
UPPER(str)      -- Convert to uppercase
TRIM(str)       -- Remove whitespace
SUBSTRING(str, start, length) -- Extract substring
REPLACE(str, from, to) -- Replace substring
SPLIT_PART(str, delimiter, field) -- Split and extract
```

### Date/Time Functions

```sql
NOW()           -- Current timestamp
CURRENT_DATE    -- Current date
CURRENT_TIME    -- Current time
EXTRACT(field FROM timestamp) -- Extract field
DATE_TRUNC(precision, timestamp) -- Truncate to precision
AGE(timestamp)  -- Interval from now
```

### JSON Functions

```sql
JSON_BUILD_OBJECT(key, value, ...) -- Build JSON object
JSON_BUILD_ARRAY(value, ...) -- Build JSON array
JSON_EXTRACT_PATH(json, path) -- Extract value at path
JSONB_SET(jsonb, path, value) -- Set value at path
JSONB_INSERT(jsonb, path, value) -- Insert value
```

## Advanced Features

### Views

```sql
CREATE VIEW active_users AS
    SELECT * FROM users WHERE last_login > NOW() - INTERVAL '30 days';

-- Materialized view
CREATE MATERIALIZED VIEW user_stats AS
    SELECT country, COUNT(*) as user_count
    FROM users
    GROUP BY country;

REFRESH MATERIALIZED VIEW user_stats;
```

### Triggers

```sql
CREATE TRIGGER update_timestamp
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### Stored Procedures

```sql
CREATE FUNCTION add_user(email TEXT, name TEXT)
RETURNS INTEGER AS $$
DECLARE
    new_id INTEGER;
BEGIN
    INSERT INTO users (email, name) VALUES (email, name)
    RETURNING id INTO new_id;
    RETURN new_id;
END;
$$ LANGUAGE plpgsql;

-- Call function
SELECT add_user('user@example.com', 'John');
```

### Transactions

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- Savepoints
BEGIN;
INSERT INTO users (email) VALUES ('user1@example.com');
SAVEPOINT sp1;
INSERT INTO users (email) VALUES ('user2@example.com');
ROLLBACK TO sp1;
COMMIT;

-- Isolation levels
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

### Full-Text Search

```sql
-- Create text search vector
ALTER TABLE documents ADD COLUMN tsv tsvector;
UPDATE documents SET tsv = to_tsvector('english', title || ' ' || body);
CREATE INDEX idx_documents_tsv ON documents USING GIN(tsv);

-- Search
SELECT * FROM documents 
WHERE tsv @@ to_tsquery('english', 'postgres & database');

-- Ranking
SELECT *, ts_rank(tsv, query) AS rank
FROM documents, to_tsquery('english', 'postgres') query
WHERE tsv @@ query
ORDER BY rank DESC;
```

## Performance Hints

### EXPLAIN

```sql
EXPLAIN SELECT * FROM users WHERE age > 18;
EXPLAIN ANALYZE SELECT * FROM users WHERE age > 18;
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM users WHERE age > 18;
```

### Index Hints (via configuration)

```sql
SET enable_seqscan = off;  -- Force index usage
SET enable_hashjoin = off; -- Disable hash joins
```

### Parallel Query

```sql
SET max_parallel_workers_per_gather = 4;
SELECT /*+ Parallel(users 4) */ * FROM users;
```
