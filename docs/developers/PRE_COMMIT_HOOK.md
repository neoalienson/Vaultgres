# Pre-Commit Hook Setup

## Overview

RustGres uses a pre-commit hook to ensure code quality before commits. The hook performs:

1. **Secret scanning** - Detects credentials and sensitive information
2. **Code formatting** - Ensures consistent code style with `rustfmt`
3. **Linting** - Catches common errors with `clippy`

## Installation

### Automatic Setup

```bash
# From repository root
cp scripts/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

### Manual Setup

```bash
# Create symlink (recommended - auto-updates with script changes)
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
```

## Requirements

### Required
- **Rust toolchain** (rustc, cargo, rustfmt, clippy)

### Optional
- **gitleaks** - Professional secret scanning tool
  ```bash
  # macOS
  brew install gitleaks
  
  # Linux
  wget https://github.com/gitleaks/gitleaks/releases/download/v8.18.0/gitleaks_8.18.0_linux_x64.tar.gz
  tar -xzf gitleaks_8.18.0_linux_x64.tar.gz
  sudo mv gitleaks /usr/local/bin/
  ```

## What Gets Checked

### 1. Secret Scanning
- **With gitleaks**: Detects 1000+ secret patterns (AWS keys, GitHub tokens, private keys, etc.)
- **Without gitleaks**: Basic regex patterns for common credentials

Patterns detected:
- `password=`, `secret=`, `api_key=`, `token=`
- `credential=`, `private_key=`

### 2. Code Formatting
Runs: `cargo fmt --all -- --check`

Ensures all code follows Rust formatting standards.

**Fix**: `cargo fmt --all`

### 3. Clippy Linting
Runs: `cargo clippy --all-targets --all-features -- -W clippy::all`

Catches common errors and code quality issues.

**Fix**: `cargo clippy --fix`

## Usage

The hook runs automatically on `git commit`. Example output:

```
Running pre-commit checks...
Checking for sensitive information...
✓ No secrets found
Checking code formatting...
✓ Formatting OK
Running clippy...
✓ Clippy OK
```

### Bypass Hook (Emergency Only)

```bash
# Skip pre-commit checks (NOT recommended)
git commit --no-verify -m "message"
```

## Troubleshooting

### Hook Not Running
```bash
# Verify hook is executable
ls -l .git/hooks/pre-commit
# Should show: -rwxr-xr-x

# Make executable if needed
chmod +x .git/hooks/pre-commit
```

### Formatting Failures
```bash
# Auto-fix formatting
cargo fmt --all

# Check what would change
cargo fmt --all -- --check
```

### Clippy Failures
```bash
# Auto-fix clippy issues
cargo clippy --fix --all-targets --all-features

# See all warnings
cargo clippy --all-targets --all-features
```

### Secret Detection False Positives

If gitleaks reports false positives, create `.gitleaks.toml`:

```toml
[allowlist]
paths = [
    "tests/fixtures/",
    "docs/examples/"
]

regexes = [
    "example_key_12345"  # Test data
]
```

## CI Integration

The same checks run in CI (`.github/workflows/ci.yml`):
- Formatting check fails the build
- Clippy warnings are reported but don't fail the build

## Best Practices

1. **Run checks before committing**:
   ```bash
   cargo fmt --all && cargo clippy --fix
   ```

2. **Install gitleaks** for better secret detection

3. **Don't bypass the hook** unless absolutely necessary

4. **Keep the hook updated**:
   ```bash
   # If using symlink, just pull latest changes
   git pull
   
   # If using copy, re-copy the script
   cp scripts/pre-commit .git/hooks/pre-commit
   ```

## For New Contributors

Add this to your onboarding checklist:

```bash
# 1. Clone repository
git clone https://github.com/rustgres/rustgres.git
cd rustgres

# 2. Install pre-commit hook
cp scripts/pre-commit .git/hooks/pre-commit

# 3. Install gitleaks (optional but recommended)
brew install gitleaks  # macOS
# or download from https://github.com/gitleaks/gitleaks/releases

# 4. Verify setup
.git/hooks/pre-commit
```
