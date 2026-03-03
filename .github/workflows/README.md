# GitHub Actions CI/CD

This document describes the GitHub Actions workflows for VaultGres.

## Workflows

### CI (`ci.yml`)

Runs on every push and pull request to `main` branch.

**Jobs:**

1. **Unit Tests**
   - Runs all unit tests (`cargo test --lib`)
   - Uses caching for faster builds
   - Reports: Test results in workflow logs

2. **Lint**
   - Checks code formatting (`cargo fmt --check`)
   - Runs Clippy linter (`cargo clippy -- -D warnings`)
   - Fails if any warnings or formatting issues

3. **Test Coverage**
   - Generates coverage report using `cargo-tarpaulin`
   - Uploads to Codecov
   - Archives coverage report as artifact
   - Reports: Coverage percentage and detailed report

### Integration Tests (`integration.yml`)

Runs integration tests separately for better organization.

**Jobs:**

1. **Integration Tests**
   - Runs integration test suite
   - Tests cross-module functionality

### Release (`release.yml`)

Triggered on version tags (e.g., `v0.2.0`).

**Jobs:**

1. **Build Release**
   - Builds optimized binaries for Linux and macOS
   - Strips debug symbols
   - Creates GitHub release with binaries
   - Uploads artifacts

## Badges

Add these badges to your README.md:

```markdown
[![CI](https://github.com/YOUR_USERNAME/vaultgres/workflows/CI/badge.svg)](https://github.com/YOUR_USERNAME/vaultgres/actions/workflows/ci.yml)
[![Integration Tests](https://github.com/YOUR_USERNAME/vaultgres/workflows/Integration%20Tests/badge.svg)](https://github.com/YOUR_USERNAME/vaultgres/actions/workflows/integration.yml)
[![codecov](https://codecov.io/gh/YOUR_USERNAME/vaultgres/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USERNAME/vaultgres)
```

## Setup

### Codecov Integration

1. Sign up at [codecov.io](https://codecov.io)
2. Add your repository
3. No token needed for public repos
4. For private repos, add `CODECOV_TOKEN` to repository secrets

### Release Process

1. Update version in `Cargo.toml`
2. Commit changes
3. Create and push tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
4. GitHub Actions will automatically build and create release

## Local Testing

Test workflows locally before pushing:

```bash
# Install act (GitHub Actions local runner)
brew install act  # macOS
# or
curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash

# Run workflows locally
act -j test        # Run unit tests
act -j lint        # Run linting
act -j coverage    # Run coverage (requires Docker)
```

## Caching

All workflows use GitHub Actions caching to speed up builds:
- Cargo registry cache
- Cargo git cache
- Build target cache

Cache is invalidated when `Cargo.lock` changes.

## Troubleshooting

### Coverage Job Fails

If `cargo-tarpaulin` installation fails, it may be due to system dependencies. The workflow will continue even if coverage fails (`fail_ci_if_error: false`).

### Clippy Warnings

The lint job fails on any Clippy warnings (`-D warnings`). Fix warnings locally:

```bash
cargo clippy --fix --all-targets --all-features
```

### Cache Issues

If builds are slow or failing due to cache corruption:

1. Go to Actions → Caches
2. Delete old caches
3. Re-run workflow

## Performance

Typical workflow times:
- **Unit Tests**: 2-3 minutes (with cache)
- **Lint**: 1-2 minutes (with cache)
- **Coverage**: 5-7 minutes
- **Integration Tests**: 3-4 minutes

First run (no cache): 10-15 minutes per job.
