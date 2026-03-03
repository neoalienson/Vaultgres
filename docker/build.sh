#!/bin/bash
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Building VaultGres Docker Image${NC}"

# Get version from Cargo.toml
VERSION=$(grep '^version' ../Cargo.toml | head -1 | cut -d'"' -f2)
echo -e "${YELLOW}Version: ${VERSION}${NC}"

# Build image
echo -e "${YELLOW}Building image...${NC}"
docker build -f Dockerfile -t vaultgres:${VERSION} -t vaultgres:latest ..

# Get image size
SIZE=$(docker images vaultgres:latest --format "{{.Size}}")
echo -e "${GREEN}Image built successfully!${NC}"
echo -e "${GREEN}Image size: ${SIZE}${NC}"

# Security scan (if trivy is installed)
if command -v trivy &> /dev/null; then
    echo -e "${YELLOW}Running security scan...${NC}"
    trivy image --severity HIGH,CRITICAL vaultgres:latest
fi

echo -e "${GREEN}Done!${NC}"
echo -e "${YELLOW}Run with: docker run -d -p 5432:5432 vaultgres:latest${NC}"
