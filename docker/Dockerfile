# Multi-stage build for minimal and secure image
FROM rust:1.82-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev

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

# Create data directory
WORKDIR /var/lib/vaultgres

# Use non-root user (from distroless)
USER nonroot:nonroot

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/vaultgres", "--version"]

# Run the database
ENTRYPOINT ["/usr/local/bin/vaultgres"]
CMD ["start", "-D", "/var/lib/vaultgres/data", "-p", "5432"]
