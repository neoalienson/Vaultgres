# Contributing Guide

Thank you for your interest in contributing to RustGres! This guide will help you get started.

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to Contribute

### 1. Report Bugs

Found a bug? Please [open an issue](https://github.com/rustgres/rustgres/issues/new) with:
- Clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- RustGres version and OS
- Relevant logs or error messages

### 2. Suggest Features

Have an idea? [Open a feature request](https://github.com/rustgres/rustgres/issues/new) with:
- Use case and motivation
- Proposed solution
- Alternative approaches considered
- Impact on existing functionality

### 3. Improve Documentation

Documentation improvements are always welcome:
- Fix typos or unclear explanations
- Add examples
- Improve API documentation
- Write tutorials or guides

### 4. Write Code

Ready to code? See [Development Setup](#development-setup) below.

## Development Setup

### Prerequisites

```bash
# Install Rust (1.75+)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install development tools
rustup component add rustfmt clippy

# Install dependencies
# Ubuntu/Debian
sudo apt install build-essential pkg-config libssl-dev

# macOS
xcode-select --install
brew install openssl pkg-config
```

### Clone and Build

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/rustgres.git
cd rustgres

# Add upstream remote
git remote add upstream https://github.com/rustgres/rustgres.git

# Build
cargo build

# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run
```

### Project Structure

```
rustgres/
├── src/
│   ├── main.rs              # Entry point
│   ├── protocol/            # PostgreSQL wire protocol
│   ├── parser/              # SQL parser
│   ├── optimizer/           # Query optimizer
│   ├── executor/            # Query executor
│   ├── storage/             # Storage engine
│   ├── transaction/         # Transaction manager
│   └── catalog/             # System catalog
├── tests/                   # Integration tests
├── benches/                 # Benchmarks
├── docs/                    # Documentation
└── examples/                # Example code
```

## Development Workflow

### 1. Pick an Issue

- Browse [good first issues](https://github.com/rustgres/rustgres/labels/good%20first%20issue)
- Comment on the issue to claim it
- Ask questions if anything is unclear

### 2. Create a Branch

```bash
# Update main
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/my-feature
# or
git checkout -b fix/issue-123
```

### 3. Make Changes

**Code Style**:
```bash
# Format code
cargo fmt

# Run linter
cargo clippy -- -D warnings

# Check for common mistakes
cargo clippy --all-targets --all-features
```

**Write Tests**:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_my_feature() {
        // Arrange
        let input = "test";
        
        // Act
        let result = my_function(input);
        
        // Assert
        assert_eq!(result, expected);
    }
}
```

**Document Code**:
```rust
/// Parses a SQL SELECT statement.
///
/// # Arguments
///
/// * `input` - The SQL string to parse
///
/// # Returns
///
/// A `SelectStmt` AST node
///
/// # Errors
///
/// Returns `ParseError` if the input is not valid SQL
///
/// # Examples
///
/// ```
/// let stmt = parse_select("SELECT * FROM users")?;
/// ```
pub fn parse_select(input: &str) -> Result<SelectStmt, ParseError> {
    // Implementation
}
```

### 4. Test Your Changes

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_my_feature

# Run with output
cargo test -- --nocapture

# Run integration tests
cargo test --test integration

# Run benchmarks
cargo bench
```

### 5. Commit Changes

**Commit Message Format**:
```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting
- `refactor`: Code restructuring
- `perf`: Performance improvement
- `test`: Add/update tests
- `chore`: Maintenance

**Example**:
```
feat(optimizer): add join reordering optimization

Implement dynamic programming algorithm for optimal join ordering.
Uses cost-based estimation to select best join order.

Closes #123
```

**Commit**:
```bash
git add .
git commit -m "feat(optimizer): add join reordering"
```

### 6. Push and Create PR

```bash
# Push to your fork
git push origin feature/my-feature

# Create pull request on GitHub
# Fill in the PR template
```

## Pull Request Guidelines

### PR Checklist

- [ ] Code follows project style (cargo fmt, cargo clippy)
- [ ] Tests added/updated and passing
- [ ] Documentation updated
- [ ] Commit messages follow convention
- [ ] PR description explains changes
- [ ] Linked to related issue(s)

### PR Template

```markdown
## Description
Brief description of changes

## Motivation
Why is this change needed?

## Changes
- Change 1
- Change 2

## Testing
How was this tested?

## Checklist
- [ ] Tests pass
- [ ] Documentation updated
- [ ] No breaking changes (or documented)

Closes #123
```

### Review Process

1. **Automated checks**: CI runs tests, linting, benchmarks
2. **Code review**: Maintainer reviews code
3. **Feedback**: Address review comments
4. **Approval**: Maintainer approves PR
5. **Merge**: Maintainer merges PR

## Coding Standards

### Rust Style

Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/):

```rust
// Use descriptive names
fn calculate_join_cost(left: &Cost, right: &Cost) -> Cost { }

// Prefer iterators over loops
let sum: i32 = values.iter().sum();

// Use Result for errors
fn parse_query(sql: &str) -> Result<Query, ParseError> { }

// Use Option for nullable values
fn find_user(id: i32) -> Option<User> { }

// Document public APIs
/// Executes a SQL query.
pub fn execute(query: &str) -> Result<ResultSet> { }
```

### Error Handling

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("page not found: {0}")]
    PageNotFound(PageId),
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("transaction aborted")]
    TransactionAborted,
}

// Use ? operator
fn read_page(id: PageId) -> Result<Page, StorageError> {
    let data = std::fs::read(format!("page_{}", id))?;
    Ok(Page::from_bytes(&data)?)
}
```

### Testing

```rust
// Unit tests in same file
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_case() {
        assert_eq!(add(2, 2), 4);
    }
    
    #[test]
    #[should_panic(expected = "overflow")]
    fn test_overflow() {
        add(i32::MAX, 1);
    }
}

// Integration tests in tests/
// tests/integration_test.rs
use rustgres::*;

#[test]
fn test_end_to_end() {
    let db = Database::new();
    db.execute("CREATE TABLE users (id INT)").unwrap();
    db.execute("INSERT INTO users VALUES (1)").unwrap();
    let result = db.execute("SELECT * FROM users").unwrap();
    assert_eq!(result.rows.len(), 1);
}
```

### Benchmarking

```rust
// benches/my_benchmark.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rustgres::*;

fn benchmark_query(c: &mut Criterion) {
    let db = setup_database();
    
    c.bench_function("select_1000_rows", |b| {
        b.iter(|| {
            db.execute(black_box("SELECT * FROM users LIMIT 1000"))
        });
    });
}

criterion_group!(benches, benchmark_query);
criterion_main!(benches);
```

## Documentation

### Code Documentation

```rust
//! Module-level documentation
//!
//! This module implements the query optimizer.

/// Function documentation
///
/// # Examples
///
/// ```
/// use rustgres::optimizer::optimize;
/// let plan = optimize(query)?;
/// ```
pub fn optimize(query: Query) -> Result<Plan> { }
```

### User Documentation

Located in `docs/`:
- Use Markdown format
- Include code examples
- Add diagrams where helpful
- Keep it up to date with code

### Generate Documentation

```bash
# Generate API docs
cargo doc --no-deps --open

# Check for broken links
cargo doc --no-deps 2>&1 | grep warning
```

## Performance

### Profiling

```bash
# CPU profiling with perf
cargo build --release
perf record --call-graph=dwarf ./target/release/rustgres
perf report

# Memory profiling with valgrind
cargo build
valgrind --tool=massif ./target/debug/rustgres
ms_print massif.out.*

# Flamegraph
cargo install flamegraph
cargo flamegraph --bench my_benchmark
```

### Benchmarking

```bash
# Run benchmarks
cargo bench

# Compare with baseline
cargo bench -- --save-baseline main
# Make changes
cargo bench -- --baseline main
```

## Release Process

### Version Numbering

Follow [Semantic Versioning](https://semver.org/):
- **Major**: Breaking changes
- **Minor**: New features, backward compatible
- **Patch**: Bug fixes, backward compatible

### Release Checklist

1. Update version in `Cargo.toml`
2. Update `CHANGELOG.md`
3. Run full test suite
4. Update documentation
5. Create git tag
6. Build release binaries
7. Publish to crates.io
8. Create GitHub release

## Getting Help

- **Discord**: Join our [Discord server](https://discord.gg/rustgres)
- **Discussions**: Use [GitHub Discussions](https://github.com/rustgres/rustgres/discussions)
- **Issues**: Search [existing issues](https://github.com/rustgres/rustgres/issues)
- **Email**: rustgres-dev@example.com

## Recognition

Contributors are recognized in:
- `CONTRIBUTORS.md` file
- Release notes
- Project website

Thank you for contributing to RustGres! 🦀
