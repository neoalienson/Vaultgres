#!/bin/bash

set -e

echo "Renaming project from vaultgres to vaultgres..."

# Find and replace in all files (case-sensitive)
find . -type f \( \
    -name "*.rs" -o \
    -name "*.toml" -o \
    -name "*.md" -o \
    -name "*.yaml" -o \
    -name "*.yml" -o \
    -name "*.sh" -o \
    -name "Dockerfile" \
\) -not -path "./target/*" -not -path "./.git/*" -exec sed -i 's/vaultgres/vaultgres/g' {} +

# Replace VaultGres with VaultGres (PascalCase)
find . -type f \( \
    -name "*.rs" -o \
    -name "*.toml" -o \
    -name "*.md" -o \
    -name "*.yaml" -o \
    -name "*.yml" -o \
    -name "*.sh" -o \
    -name "Dockerfile" \
\) -not -path "./target/*" -not -path "./.git/*" -exec sed -i 's/VaultGres/VaultGres/g' {} +

# Replace VAULTGRES with VAULTGRES (uppercase)
find . -type f \( \
    -name "*.rs" -o \
    -name "*.toml" -o \
    -name "*.md" -o \
    -name "*.yaml" -o \
    -name "*.yml" -o \
    -name "*.sh" -o \
    -name "Dockerfile" \
\) -not -path "./target/*" -not -path "./.git/*" -exec sed -i 's/VAULTGRES/VAULTGRES/g' {} +

echo "Text replacements complete!"
echo ""
echo "Manual steps required:"
echo "1. Rename directory: cd .. && mv vaultgres vaultgres && cd vaultgres"
echo "2. Update git remote if needed"
echo "3. Run: cargo clean && cargo build"
echo "4. Run: cargo test"
echo ""
echo "Done!"
