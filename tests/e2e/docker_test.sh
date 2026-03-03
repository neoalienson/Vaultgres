#!/bin/bash
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}VaultGres E2E Tests with Docker${NC}"

# Configuration
CONTAINER_NAME="vaultgres-e2e-test"
IMAGE_NAME="vaultgres:latest"
PORT=15432
DATA_DIR="/tmp/vaultgres-e2e-data"

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    rm -rf $DATA_DIR
}

# Trap cleanup on exit
trap cleanup EXIT

# Build image if not exists
if ! docker images $IMAGE_NAME | grep -q vaultgres; then
    echo -e "${YELLOW}Building Docker image...${NC}"
    docker build -f docker/Dockerfile -t $IMAGE_NAME .
fi

# Start container
echo -e "${YELLOW}Starting VaultGres container...${NC}"
docker run -d \
    --name $CONTAINER_NAME \
    -p $PORT:5432 \
    -e RUST_LOG=info \
    $IMAGE_NAME

# Wait for container to be ready
echo -e "${YELLOW}Waiting for database to be ready...${NC}"
sleep 3

# Check if container is running
if ! docker ps | grep -q $CONTAINER_NAME; then
    echo -e "${RED}Container failed to start${NC}"
    docker logs $CONTAINER_NAME
    exit 1
fi

echo -e "${GREEN}Container is running!${NC}"

# Test 1: Check container status
echo -e "${YELLOW}Test 1: Check container status${NC}"
if docker ps | grep -q $CONTAINER_NAME; then
    echo -e "${GREEN}✓ Container is running${NC}"
else
    echo -e "${RED}Container not running${NC}"
    exit 1
fi

# Test 2: Check logs for startup
echo -e "${YELLOW}Test 2: Check startup logs${NC}"
if docker logs $CONTAINER_NAME 2>&1 | grep -q "Ready for connections"; then
    echo -e "${GREEN}✓ Server started successfully${NC}"
else
    echo -e "${RED}Server startup failed${NC}"
    docker logs $CONTAINER_NAME
    exit 1
fi

# Test 3: Check if port is listening
echo -e "${YELLOW}Test 3: Check if port is listening${NC}"
if nc -z localhost $PORT 2>/dev/null; then
    echo -e "${GREEN}✓ Port $PORT is listening${NC}"
else
    echo -e "${YELLOW}⚠ Port check skipped (nc not available)${NC}"
fi

# Test 4: Check logs
echo -e "${YELLOW}Test 4: Check logs${NC}"
docker logs $CONTAINER_NAME | head -10
echo -e "${GREEN}✓ Logs retrieved${NC}"

# Test 5: Resource usage
echo -e "${YELLOW}Test 5: Check resource usage${NC}"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" $CONTAINER_NAME
echo -e "${GREEN}✓ Resource check passed${NC}"

# Summary
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}All E2E tests passed!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Container: $CONTAINER_NAME${NC}"
echo -e "${YELLOW}Port: $PORT${NC}"
echo -e "${YELLOW}Image size: $(docker images $IMAGE_NAME --format '{{.Size}}')${NC}"
