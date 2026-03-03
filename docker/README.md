# VaultGres Docker Deployment

Minimal and secure Docker image for VaultGres database.

## Image Features

- **Minimal Size**: Multi-stage build with distroless base (~15MB)
- **Security**: 
  - Non-root user (distroless nonroot)
  - Read-only filesystem
  - Dropped capabilities
  - No shell or package manager
  - Static binary (no dependencies)
- **Production Ready**: Health checks and proper signal handling

## Quick Start

### Build Image

```bash
# From project root
docker build -f docker/Dockerfile -t vaultgres:latest .
```

### Run Container

```bash
docker run -d \
  --name vaultgres \
  -p 5432:5432 \
  -v vaultgres-data:/var/lib/vaultgres/data \
  vaultgres:latest
```

### Using Docker Compose

```bash
cd docker
docker-compose up -d
```

## Configuration

### Environment Variables

- `RUST_LOG`: Log level (debug, info, warn, error)

### Volumes

- `/var/lib/vaultgres/data`: Database data directory
- `/var/lib/vaultgres/wal`: Write-ahead log directory

### Ports

- `5432`: PostgreSQL protocol port

## Connect to Database

```bash
# Using psql
psql -h localhost -p 5432 -U postgres

# Using Docker exec
docker exec -it vaultgres vaultgres --help
```

## Health Check

```bash
docker inspect --format='{{.State.Health.Status}}' vaultgres
```

## Security Best Practices

1. **Network Isolation**: Use Docker networks
2. **Secrets Management**: Use Docker secrets or environment files
3. **Resource Limits**: Set memory and CPU limits
4. **Regular Updates**: Rebuild image with latest base

## Production Deployment

```bash
# Build optimized image
docker build -f docker/Dockerfile -t vaultgres:0.2.0 .

# Run with resource limits
docker run -d \
  --name vaultgres \
  -p 5432:5432 \
  -v vaultgres-data:/var/lib/vaultgres/data \
  --memory="2g" \
  --cpus="2" \
  --restart=unless-stopped \
  --security-opt=no-new-privileges:true \
  --cap-drop=ALL \
  --cap-add=NET_BIND_SERVICE \
  --read-only \
  --tmpfs /tmp:noexec,nosuid,size=100m \
  vaultgres:0.2.0
```

## Troubleshooting

### View Logs

```bash
docker logs vaultgres
docker logs -f vaultgres  # Follow logs
```

### Check Container Status

```bash
docker ps -a | grep vaultgres
docker inspect vaultgres
```

### Access Container (Debug)

Note: Distroless images don't have a shell. For debugging, use a debug variant:

```dockerfile
FROM gcr.io/distroless/static:debug-nonroot
```

## Security Scan

```bash
# Scan for vulnerabilities
docker scan vaultgres:latest

# Using trivy
trivy image vaultgres:latest
```
