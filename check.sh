#!/bin/bash
set -e

echo "=== Formatting ==="
pnpm format

echo ""
echo "=== Linting ==="
pnpm lint

echo ""
echo "=== Type checking ==="
pnpm typecheck

echo ""
echo "=== Running tests ==="
pnpm test

echo ""
echo "=== All checks passed ==="
