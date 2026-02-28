# Installation Guide

Complete guide to installing and configuring RustGres.

## System Requirements

### Minimum Requirements
- **CPU**: 2 cores
- **RAM**: 512 MB
- **Disk**: 1 GB free space
- **OS**: Linux (kernel 4.14+), macOS (10.15+), Windows (10+)

### Recommended Requirements
- **CPU**: 4+ cores
- **RAM**: 4 GB+
- **Disk**: SSD with 10 GB+ free space
- **OS**: Linux (kernel 5.10+) with io_uring support

### Software Requirements
- **Rust**: 1.75+ (for building from source)
- **GCC/Clang**: For native dependencies
- **Git**: For source checkout

## Installation Methods

### 1. Binary Installation (Recommended)

**Linux (x86_64)**:
```bash
curl -L https://github.com/rustgres/rustgres/releases/latest/download/rustgres-linux-x64.tar.gz | tar xz
sudo mv rustgres /usr/local/bin/
sudo chmod +x /usr/local/bin/rustgres
```

**macOS (Apple Silicon)**:
```bash
curl -L https://github.com/rustgres/rustgres/releases/latest/download/rustgres-macos-arm64.tar.gz | tar xz
sudo mv rustgres /usr/local/bin/
sudo chmod +x /usr/local/bin/rustgres
```

**macOS (Intel)**:
```bash
curl -L https://github.com/rustgres/rustgres/releases/latest/download/rustgres-macos-x64.tar.gz | tar xz
sudo mv rustgres /usr/local/bin/
sudo chmod +x /usr/local/bin/rustgres
```

**Windows**:
```powershell
# Download from GitHub releases
Invoke-WebRequest -Uri https://github.com/rustgres/rustgres/releases/latest/download/rustgres-windows-x64.zip -OutFile rustgres.zip
Expand-Archive rustgres.zip -DestinationPath C:\rustgres
# Add C:\rustgres to PATH
```

### 2. Package Managers

**Homebrew (macOS/Linux)**:
```bash
brew tap rustgres/tap
brew install rustgres
```

**APT (Debian/Ubuntu)**:
```bash
curl -fsSL https://packages.rustgres.org/gpg | sudo gpg --dearmor -o /usr/share/keyrings/rustgres.gpg
echo "deb [signed-by=/usr/share/keyrings/rustgres.gpg] https://packages.rustgres.org/apt stable main" | sudo tee /etc/apt/sources.list.d/rustgres.list
sudo apt update
sudo apt install rustgres
```

**YUM/DNF (RHEL/CentOS/Fedora)**:
```bash
sudo dnf config-manager --add-repo https://packages.rustgres.org/rpm/rustgres.repo
sudo dnf install rustgres
```

**Arch Linux (AUR)**:
```bash
yay -S rustgres
# or
paru -S rustgres
```

**Docker**:
```bash
docker pull rustgres/rustgres:latest
docker run -d -p 5432:5432 --name rustgres rustgres/rustgres:latest
```

### 3. Build from Source

**Prerequisites**:
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Install build dependencies
# Ubuntu/Debian
sudo apt install build-essential pkg-config libssl-dev

# macOS
xcode-select --install
brew install openssl pkg-config

# Fedora/RHEL
sudo dnf install gcc openssl-devel pkg-config
```

**Build**:
```bash
# Clone repository
git clone https://github.com/rustgres/rustgres.git
cd rustgres

# Build release binary
cargo build --release

# Install
sudo cp target/release/rustgres /usr/local/bin/
sudo cp target/release/rustgres-ctl /usr/local/bin/

# Verify installation
rustgres --version
```

**Build with optimizations**:
```bash
# Maximum performance (slower build)
RUSTFLAGS="-C target-cpu=native" cargo build --release

# With LTO (Link-Time Optimization)
cargo build --release --config profile.release.lto=true

# Minimal binary size
cargo build --release --config profile.release.opt-level='z'
```

## Initial Setup

### 1. Create System User

```bash
# Linux
sudo useradd -r -s /bin/bash -d /var/lib/rustgres rustgres
sudo mkdir -p /var/lib/rustgres
sudo chown rustgres:rustgres /var/lib/rustgres

# macOS
sudo dscl . -create /Users/rustgres
sudo dscl . -create /Users/rustgres UserShell /bin/bash
sudo dscl . -create /Users/rustgres RealName "RustGres Server"
sudo dscl . -create /Users/rustgres NFSHomeDirectory /var/lib/rustgres
sudo mkdir -p /var/lib/rustgres
sudo chown rustgres:staff /var/lib/rustgres
```

### 2. Initialize Database

```bash
# As rustgres user
sudo -u rustgres rustgres init -D /var/lib/rustgres/data

# Or with custom options
sudo -u rustgres rustgres init \
    -D /var/lib/rustgres/data \
    --encoding=UTF8 \
    --locale=en_US.UTF-8 \
    --auth=scram-sha-256
```

**Output**:
```
The files belonging to this database system will be owned by user "rustgres".
This user must also own the server process.

The database cluster will be initialized with locale "en_US.UTF-8".
The default database encoding has accordingly been set to "UTF8".

creating directory /var/lib/rustgres/data ... ok
creating subdirectories ... ok
selecting default max_connections ... 100
selecting default shared_buffers ... 128MB
creating configuration files ... ok
running bootstrap script ... ok
performing post-bootstrap initialization ... ok
syncing data to disk ... ok

Success. You can now start the database server using:

    rustgres start -D /var/lib/rustgres/data
```

### 3. Configure Server

Edit `/var/lib/rustgres/data/rustgres.conf`:

```ini
# Connection settings
listen_addresses = 'localhost'  # Change to '*' for all interfaces
port = 5432
max_connections = 100

# Memory settings
shared_buffers = 256MB          # 25% of RAM
work_mem = 4MB
maintenance_work_mem = 64MB
effective_cache_size = 1GB      # 50-75% of RAM

# WAL settings
wal_level = replica
max_wal_size = 1GB
min_wal_size = 80MB
checkpoint_timeout = 5min

# Logging
log_destination = 'stderr'
logging_collector = on
log_directory = 'log'
log_filename = 'rustgres-%Y-%m-%d_%H%M%S.log'
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_min_duration_statement = 1000  # Log queries > 1s
```

Edit `/var/lib/rustgres/data/pg_hba.conf` for authentication:

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# Local connections
local   all             all                                     scram-sha-256

# IPv4 local connections
host    all             all             127.0.0.1/32            scram-sha-256

# IPv6 local connections
host    all             all             ::1/128                 scram-sha-256

# Remote connections (uncomment if needed)
# host    all             all             0.0.0.0/0               scram-sha-256
```

### 4. Start Server

**Foreground (for testing)**:
```bash
sudo -u rustgres rustgres start -D /var/lib/rustgres/data
```

**Background (daemon)**:
```bash
sudo -u rustgres rustgres start -D /var/lib/rustgres/data -l /var/lib/rustgres/data/log/server.log &
```

**Using systemd (Linux)**:

Create `/etc/systemd/system/rustgres.service`:
```ini
[Unit]
Description=RustGres Database Server
After=network.target

[Service]
Type=forking
User=rustgres
Group=rustgres
ExecStart=/usr/local/bin/rustgres start -D /var/lib/rustgres/data -l /var/lib/rustgres/data/log/server.log
ExecStop=/usr/local/bin/rustgres stop -D /var/lib/rustgres/data
ExecReload=/usr/local/bin/rustgres reload -D /var/lib/rustgres/data
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable rustgres
sudo systemctl start rustgres
sudo systemctl status rustgres
```

**Using launchd (macOS)**:

Create `/Library/LaunchDaemons/org.rustgres.server.plist`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>org.rustgres.server</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/rustgres</string>
        <string>start</string>
        <string>-D</string>
        <string>/var/lib/rustgres/data</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>UserName</key>
    <string>rustgres</string>
</dict>
</plist>
```

Load and start:
```bash
sudo launchctl load /Library/LaunchDaemons/org.rustgres.server.plist
sudo launchctl start org.rustgres.server
```

### 5. Create Database and User

```bash
# Create database
rustgres createdb mydb

# Create user
rustgres createuser myuser

# Set password
rustgres psql -c "ALTER USER myuser WITH PASSWORD 'mypassword';"

# Grant privileges
rustgres psql -c "GRANT ALL PRIVILEGES ON DATABASE mydb TO myuser;"
```

### 6. Verify Installation

```bash
# Check server status
rustgres status -D /var/lib/rustgres/data

# Connect with psql
psql -h localhost -p 5432 -U postgres -d postgres

# Run test query
psql -h localhost -p 5432 -U postgres -c "SELECT version();"
```

## Client Tools

### psql (PostgreSQL Client)

**Install**:
```bash
# Ubuntu/Debian
sudo apt install postgresql-client

# macOS
brew install libpq
echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.zshrc

# Windows
# Download from https://www.postgresql.org/download/windows/
```

**Connect**:
```bash
psql -h localhost -p 5432 -U postgres -d mydb
```

### GUI Tools

**pgAdmin**:
```bash
# Ubuntu/Debian
sudo apt install pgadmin4

# macOS
brew install --cask pgadmin4

# Windows
# Download from https://www.pgadmin.org/download/
```

**DBeaver**:
```bash
# Cross-platform
# Download from https://dbeaver.io/download/
```

## Docker Deployment

### Basic Container

```bash
# Run container
docker run -d \
    --name rustgres \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=mypassword \
    -v rustgres-data:/var/lib/rustgres/data \
    rustgres/rustgres:latest

# Connect
psql -h localhost -p 5432 -U postgres
```

### Docker Compose

Create `docker-compose.yml`:
```yaml
version: '3.8'

services:
  rustgres:
    image: rustgres/rustgres:latest
    container_name: rustgres
    environment:
      POSTGRES_PASSWORD: mypassword
      POSTGRES_USER: postgres
      POSTGRES_DB: mydb
    ports:
      - "5432:5432"
    volumes:
      - rustgres-data:/var/lib/rustgres/data
      - ./rustgres.conf:/var/lib/rustgres/data/rustgres.conf
    restart: unless-stopped

volumes:
  rustgres-data:
```

Start:
```bash
docker-compose up -d
```

## Kubernetes Deployment

### StatefulSet

Create `rustgres-statefulset.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: rustgres
spec:
  ports:
  - port: 5432
  clusterIP: None
  selector:
    app: rustgres
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rustgres
spec:
  serviceName: rustgres
  replicas: 1
  selector:
    matchLabels:
      app: rustgres
  template:
    metadata:
      labels:
        app: rustgres
    spec:
      containers:
      - name: rustgres
        image: rustgres/rustgres:latest
        ports:
        - containerPort: 5432
        env:
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: rustgres-secret
              key: password
        volumeMounts:
        - name: data
          mountPath: /var/lib/rustgres/data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 10Gi
```

Deploy:
```bash
kubectl apply -f rustgres-statefulset.yaml
```

## Troubleshooting

### Server won't start

**Check logs**:
```bash
tail -f /var/lib/rustgres/data/log/rustgres-*.log
```

**Common issues**:
- Port already in use: Change `port` in rustgres.conf
- Permission denied: Check file ownership and permissions
- Insufficient memory: Reduce `shared_buffers`

### Connection refused

**Check server is running**:
```bash
rustgres status -D /var/lib/rustgres/data
```

**Check listen address**:
```bash
grep listen_addresses /var/lib/rustgres/data/rustgres.conf
```

**Check firewall**:
```bash
# Linux
sudo ufw allow 5432/tcp

# macOS
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add /usr/local/bin/rustgres
```

### Authentication failed

**Check pg_hba.conf**:
```bash
cat /var/lib/rustgres/data/pg_hba.conf
```

**Reset password**:
```bash
rustgres psql -c "ALTER USER postgres WITH PASSWORD 'newpassword';"
```

## Uninstallation

### Stop server

```bash
# Systemd
sudo systemctl stop rustgres
sudo systemctl disable rustgres

# Manual
rustgres stop -D /var/lib/rustgres/data
```

### Remove files

```bash
# Remove binaries
sudo rm /usr/local/bin/rustgres*

# Remove data (WARNING: deletes all data)
sudo rm -rf /var/lib/rustgres

# Remove user
sudo userdel rustgres
```

### Remove packages

```bash
# APT
sudo apt remove rustgres

# Homebrew
brew uninstall rustgres

# Docker
docker rm -f rustgres
docker rmi rustgres/rustgres
```

## Next Steps

- [Quick Start Tutorial](QUICKSTART.md)
- [Configuration Guide](CONFIGURATION.md)
- [SQL Reference](SQL.md)
- [Administration Guide](ADMIN.md)
