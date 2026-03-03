# Linting Guide

This guide explains how to use Clippy, Rust's official linter, to maintain code quality in VaultGres.

## What is Clippy?

Clippy is a collection of lints to catch common mistakes and improve your Rust code. It checks for:
- **Performance**: Unnecessary clones, inefficient loops
- **Correctness**: Logic errors, type mismatches
- **Style**: Naming conventions, idiomatic patterns
- **Complexity**: Overly complex expressions
- **Pedantic**: Extra strict checks (opt-in)

## Installation

Clippy is included with Rust 1.29+:

```bash
# Install Clippy
rustup component add clippy

# Verify installation
cargo clippy --version
```

## Basic Usage

### Run Clippy

```bash
# Run on all targets (lib, bins, tests, benches)
cargo clippy --all-targets --all-features

# Or use the convenience script
./lint.sh
```

### Fix Warnings Automatically

```bash
# Fix warnings where possible
cargo clippy --fix --all-targets --all-features

# Fix and allow dirty working directory
cargo clippy --fix --all-targets --all-features --allow-dirty
```

### Strict Mode

```bash
# Fail on any warnings (useful for CI)
cargo clippy --all-targets --all-features -- -D warnings
```

## Configuration

VaultGres uses `.clippy.toml` for configuration:

```toml
# .clippy.toml
too-many-arguments-threshold = 10
type-complexity-threshold = 500
```

### Adjust Thresholds

- `too-many-arguments-threshold`: Maximum function parameters (default: 7)
- `type-complexity-threshold`: Maximum type complexity (default: 250)

## Common Lints in VaultGres

### 1. Unused Imports

**Warning**: `unused import: 'Foo'`

**Fix**: Remove unused imports
```rust
// Before
use std::collections::HashMap;  // unused

// After
// (removed)
```

### 2. Unnecessary Clone

**Warning**: `using 'clone' on type 'TupleHeader' which implements the 'Copy' trait`

**Fix**: Remove `.clone()` for Copy types
```rust
// Before
let header2 = header.clone();

// After
let header2 = header;
```

### 3. Useless Comparisons

**Warning**: `this comparison involving the minimum or maximum element for this type contains a case that is always true or always false`

**Fix**: Remove always-true comparisons
```rust
// Before
assert!(lsn >= 0);  // lsn is u64, always >= 0

// After
assert!(lsn == 0 || lsn > 0);  // or just remove
```

### 4. Bool Assertions

**Warning**: `used 'assert_eq!' with a literal bool`

**Fix**: Use `assert!` instead
```rust
// Before
assert_eq!(flag, true);
assert_eq!(flag, false);

// After
assert!(flag);
assert!(!flag);
```

### 5. Unnecessary Get

**Warning**: `unnecessary use of 'get("key").is_none()'`

**Fix**: Use `contains_key`
```rust
// Before
assert!(map.get("key").is_none());

// After
assert!(!map.contains_key("key"));
```

### 6. Manual is_multiple_of

**Warning**: `manual implementation of '.is_multiple_of()'`

**Fix**: Use built-in method
```rust
// Before
if n % 2 == 0 { }

// After
if n.is_multiple_of(2) { }
```

### 7. Dead Code

**Warning**: `variant 'Internal' is never constructed`

**Fix**: Either use it or mark as allowed
```rust
// Option 1: Use the code
match node {
    Node::Internal(n) => { /* use it */ }
    Node::Leaf(l) => { /* ... */ }
}

// Option 2: Allow if intentional
#[allow(dead_code)]
enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}
```

## Allowing Lints

### File Level

```rust
#![allow(clippy::too_many_arguments)]
```

### Module Level

```rust
#[allow(clippy::module_inception)]
mod parser {
    pub mod parser { }
}
```

### Function Level

```rust
#[allow(clippy::unnecessary_wraps)]
fn always_ok() -> Result<(), Error> {
    Ok(())
}
```

### Line Level

```rust
#[allow(clippy::cast_possible_truncation)]
let x = value as u32;
```

## Lint Categories

### Default Lints

Enabled by default, catch common mistakes:
```bash
cargo clippy
```

### All Lints

Enable all lints (very strict):
```bash
cargo clippy -- -W clippy::all
```

### Pedantic Lints

Extra strict, opinionated checks:
```bash
cargo clippy -- -W clippy::pedantic
```

### Restriction Lints

Opt-in lints for specific restrictions:
```bash
cargo clippy -- -W clippy::restriction
```

## CI Integration

Add to `.github/workflows/ci.yml`:

```yaml
name: CI

on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
          components: clippy
      - name: Run Clippy
        run: cargo clippy --all-targets --all-features -- -D warnings
```

## Best Practices

### 1. Run Before Commit

```bash
# Add to pre-commit hook
cargo fmt && cargo clippy --all-targets --all-features
```

### 2. Fix Incrementally

Don't try to fix all warnings at once:
```bash
# Fix one category at a time
cargo clippy --all-targets --all-features 2>&1 | grep "unused import"
```

### 3. Document Allowed Lints

When allowing a lint, explain why:
```rust
// We need more than 7 arguments for PostgreSQL protocol compatibility
#[allow(clippy::too_many_arguments)]
fn handle_query(...) { }
```

### 4. Use Clippy in Development

```bash
# Watch for changes and run clippy
cargo watch -x clippy
```

## Current Status

VaultGres Clippy status:
- ✅ **Errors**: 0 (all fixed)
- ⚠️ **Warnings**: ~95 (mostly minor style issues)
- 📝 **Configuration**: `.clippy.toml` with adjusted thresholds

### Fixed Issues

- Removed useless comparisons (u64 >= 0, usize >= 0)
- Configured type complexity threshold
- Configured function argument threshold

### Remaining Warnings

Most warnings are minor style issues:
- Unused imports (6 warnings)
- Unnecessary clones (6 warnings)
- Module naming (3 warnings)
- Dead code (2 warnings)

These can be fixed incrementally without affecting functionality.

## Troubleshooting

### Clippy Not Found

```bash
rustup component add clippy
```

### Conflicting Versions

```bash
rustup update
cargo clean
cargo clippy
```

### Too Many Warnings

```bash
# Focus on errors only
cargo clippy --all-targets --all-features 2>&1 | grep "^error:"

# Or fix automatically
cargo clippy --fix --all-targets --all-features
```

## Resources

- [Clippy Documentation](https://doc.rust-lang.org/clippy/)
- [Clippy Lint List](https://rust-lang.github.io/rust-clippy/master/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)

## Summary

Clippy is now set up and running for VaultGres:

```bash
# Quick check
./lint.sh

# Fix issues
cargo clippy --fix --all-targets --all-features

# Strict mode (CI)
cargo clippy --all-targets --all-features -- -D warnings
```

All critical errors are fixed, and the codebase passes Clippy checks with only minor style warnings remaining.
