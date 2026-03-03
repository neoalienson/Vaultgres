# Installation Guide

Complete guide to installing and configuring VaultGres.

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
curl -L https://github.com/vaultgres/vaultgres/releases/latest/download/vaultgres-linux-x64.tar.gz | tar xz
sudo mv vaultgres /usr/local/bin/
sudo chmod +x /usr/local/bin/vaultgres
```

**macOS (Apple Silicon)**:
```bash
curl -L https://github.com/vaultgres/vaultgres/releases/latest/download/vaultgres-macos-arm64.tar.gz | tar xz
sudo mv vaultgres /usr/local/bin/
sudo chmod +x /usr/local/bin/vaultgres
```

**macOS (Intel)**:
```bash
curl -L https://github.com/vaultgres/vaultgres/releases/latest/download/vaultgres-macos-x64.tar.gz | tar xz
sudo mv vaultgres /usr/local/bin/
sudo chmod +x /usr/local/bin/vaultgres
```

**Windows**:
```powershell
# Download from GitHub releases
Invoke-WebRequest -Uri https://github.com/vaultgres/vaultgres/releases/latest/download/vaultgres-windows-x64.zip -OutFile vaultgres.zip
Expand-Archive vaultgres.zip -DestinationPath C:\vaultgres
# Add C:\vaultgres to PATH
```

### 2. Package Managers

**Homebrew (macOS/Linux)**:
```bash
brew tap vaultgres/tap
brew install vaultgres
```

**APT (Debian/Ubuntu)**:
```bash
curl -fsSL https://packages.vaultgres.org/gpg | sudo gpg --dearmor -o /usr/share/keyrings/vaultgres.gpg
echo "deb [signed-by=/usr/share/keyrings/vaultgres.gpg] https://packages.vaultgres.org/apt stable main" | sudo tee /etc/apt/sources.list.d/vaultgres.list
sudo apt update
sudo apt install vaultgres
```

**YUM/DNF (RHEL/CentOS/Fedora)**:
```bash
sudo dnf config-manager --add-repo https://packages.vaultgres.org/rpm/vaultgres.repo
sudo dnf install vaultgres
```

**Arch Linux (AUR)**:
```bash
yay -S vaultgres
# or
paru -S vaultgres
```

**Docker**:
```bash
docker pull vaultgres/vaultgres:latest
docker run -d -p 5432:5432 --name vaultgres vaultgres/vaultgres:latest
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
git clone https://github.com/vaultgres/vaultgres.git
cd vaultgres

# Build release binary
cargo build --release

# Install
sudo cp target/release/vaultgres /usr/local/bin/
sudo cp target/release/vaultgres-ctl /usr/local/bin/

# Verify installation
vaultgres --version
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
sudo useradd -r -s /bin/bash -d /var/lib/vaultgres vaultgres
sudo mkdir -p /var/lib/vaultgres
sudo chown vaultgres:vaultgres /var/lib/vaultgres

# macOS
sudo dscl . -create /Users/vaultgres
sudo dscl . -create /Users/vaultgres UserShell /bin/bash
sudo dscl . -create /Users/vaultgres RealName "VaultGres Server"
sudo dscl . -create /Users/vaultgres NFSHomeDirectory /var/lib/vaultgres
sudo mkdir -p /var/lib/vaultgres
sudo chown vaultgres:staff /var/lib/vaultgres
```

### 2. Initialize Database

```bash
# As vaultgres user
sudo -u vaultgres vaultgres init -D /var/lib/vaultgres/data

# Or with custom options
sudo -u vaultgres vaultgres init \
    -D /var/lib/vaultgres/data \
    --encoding=UTF8 \
    --locale=en_US.UTF-8 \
    --auth=scram-sha-256
```

**Output**:
```
The files belonging to this database system will be owned by user "vaultgres".
This user must also own the server process.

The database cluster will be initialized with locale "en_US.UTF-8".
The default database encoding has accordingly been set to "UTF8".

creating directory /var/lib/vaultgres/data ... ok
creating subdirectories ... ok
selecting default max_connections ... 100
selecting default shared_buffers ... 128MB
creating configuration files ... ok
running bootstrap script ... ok
performing post-bootstrap initialization ... ok
syncing data to disk ... ok

Success. You can now start the database server using:

    vaultgres start -D /var/lib/vaultgres/data
```

### 3. Configure Server

Edit `/var/lib/vaultgres/data/vaultgres.conf`:

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
log_filename = 'vaultgres-%Y-%m-%d_%H%M%S.log'
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_min_duration_statement = 1000  # Log queries > 1s
```

Edit `/var/lib/vaultgres/data/pg_hba.conf` for authentication:

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
sudo -u vaultgres vaultgres start -D /var/lib/vaultgres/data
```

**Background (daemon)**:
```bash
sudo -u vaultgres vaultgres start -D /var/lib/vaultgres/data -l /var/lib/vaultgres/data/log/server.log &
```

**Using systemd (Linux)**:

Create `/etc/systemd/system/vaultgres.service`:
```ini
[Unit]
Description=VaultGres Database Server
After=network.target

[Service]
Type=forking
User=vaultgres
Group=vaultgres
ExecStart=/usr/local/bin/vaultgres start -D /var/lib/vaultgres/data -l /var/lib/vaultgres/data/log/server.log
ExecStop=/usr/local/bin/vaultgres stop -D /var/lib/vaultgres/data
ExecReload=/usr/local/bin/vaultgres reload -D /var/lib/vaultgres/data
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable vaultgres
sudo systemctl start vaultgres
sudo systemctl status vaultgres
```

**Using launchd (macOS)**:

Create `/Library/LaunchDaemons/org.vaultgres.server.plist`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>org.vaultgres.server</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/vaultgres</string>
        <string>start</string>
        <string>-D</string>
        <string>/var/lib/vaultgres/data</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>UserName</key>
    <string>vaultgres</string>
</dict>
</plist>
```

Load and start:
```bash
sudo launchctl load /Library/LaunchDaemons/org.vaultgres.server.plist
sudo launchctl start org.vaultgres.server
```

### 5. Create Database and User

```bash
# Create database
vaultgres createdb mydb

# Create user
vaultgres createuser myuser

# Set password
vaultgres psql -c "ALTER USER myuser WITH PASSWORD 'mypassword';"

# Grant privileges
vaultgres psql -c "GRANT ALL PRIVILEGES ON DATABASE mydb TO myuser;"
```

### 6. Verify Installation

```bash
# Check server status
vaultgres status -D /var/lib/vaultgres/data

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
    --name vaultgres \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=mypassword \
    -v vaultgres-data:/var/lib/vaultgres/data \
    vaultgres/vaultgres:latest

# Connect
psql -h localhost -p 5432 -U postgres
```

### Docker Compose

Create `docker-compose.yml`:
```yaml
version: '3.8'

services:
  vaultgres:
    image: vaultgres/vaultgres:latest
    container_name: vaultgres
    environment:
      POSTGRES_PASSWORD: mypassword
      POSTGRES_USER: postgres
      POSTGRES_DB: mydb
    ports:
      - "5432:5432"
    volumes:
      - vaultgres-data:/var/lib/vaultgres/data
      - ./vaultgres.conf:/var/lib/vaultgres/data/vaultgres.conf
    restart: unless-stopped

volumes:
  vaultgres-data:
```

Start:
```bash
docker-compose up -d
```

## Kubernetes Deployment

### StatefulSet

Create `vaultgres-statefulset.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: vaultgres
spec:
  ports:
  - port: 5432
  clusterIP: None
  selector:
    app: vaultgres
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: vaultgres
spec:
  serviceName: vaultgres
  replicas: 1
  selector:
    matchLabels:
      app: vaultgres
  template:
    metadata:
      labels:
        app: vaultgres
    spec:
      containers:
      - name: vaultgres
        image: vaultgres/vaultgres:latest
        ports:
        - containerPort: 5432
        env:
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: vaultgres-secret
              key: password
        volumeMounts:
        - name: data
          mountPath: /var/lib/vaultgres/data
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
kubectl apply -f vaultgres-statefulset.yaml
```

## Troubleshooting

### Server won't start

**Check logs**:
```bash
tail -f /var/lib/vaultgres/data/log/vaultgres-*.log
```

**Common issues**:
- Port already in use: Change `port` in vaultgres.conf
- Permission denied: Check file ownership and permissions
- Insufficient memory: Reduce `shared_buffers`

### Connection refused

**Check server is running**:
```bash
vaultgres status -D /var/lib/vaultgres/data
```

**Check listen address**:
```bash
grep listen_addresses /var/lib/vaultgres/data/vaultgres.conf
```

**Check firewall**:
```bash
# Linux
sudo ufw allow 5432/tcp

# macOS
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add /usr/local/bin/vaultgres
```

### Authentication failed

**Check pg_hba.conf**:
```bash
cat /var/lib/vaultgres/data/pg_hba.conf
```

**Reset password**:
```bash
vaultgres psql -c "ALTER USER postgres WITH PASSWORD 'newpassword';"
```

## Uninstallation

### Stop server

```bash
# Systemd
sudo systemctl stop vaultgres
sudo systemctl disable vaultgres

# Manual
vaultgres stop -D /var/lib/vaultgres/data
```

### Remove files

```bash
# Remove binaries
sudo rm /usr/local/bin/vaultgres*

# Remove data (WARNING: deletes all data)
sudo rm -rf /var/lib/vaultgres

# Remove user
sudo userdel vaultgres
```

### Remove packages

```bash
# APT
sudo apt remove vaultgres

# Homebrew
brew uninstall vaultgres

# Docker
docker rm -f vaultgres
docker rmi vaultgres/vaultgres
```

## Next Steps

- [Quick Start Tutorial](QUICKSTART.md)
- [Configuration Guide](CONFIGURATION.md)
- [SQL Reference](SQL.md)
- [Administration Guide](ADMIN.md)
