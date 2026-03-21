# Multi-stage build for minimal and secure image
FROM rust:1.85 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends musl-dev && rm -rf /var/lib/apt/lists/*

# Add musl target for static linking
RUN rustup target add x86_64-unknown-linux-musl

# Set git hash build-arg
ARG GIT_HASH
ENV GIT_HASH=${GIT_HASH}

# Create app directory
WORKDIR /build

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY tests ./tests
COPY benches ./benches

# Build release binary with static linking
RUN cargo build --release --target x86_64-unknown-linux-musl

# Runtime stage - use distroless for minimal attack surface
FROM gcr.io/distroless/static:nonroot

# Copy binary from builder
COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/vaultgres /usr/local/bin/vaultgres

# Copy config file
COPY docker/config.yaml /etc/vaultgres/config.yaml

# Use root user to allow writing to mounted volumes
USER root

# Set working directory
WORKDIR /var/lib/vaultgres

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/vaultgres", "--version"]

# Set config path
ENV VAULTGRES_CONFIG=/etc/vaultgres/config.yaml

# Stop signal for graceful shutdown
STOPSIGNAL SIGTERM

# Run the database
ENTRYPOINT ["/usr/local/bin/vaultgres"]
CMD ["start"]
