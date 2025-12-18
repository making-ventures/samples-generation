#!/bin/bash
set -e

echo "=== Checking for secrets with gitleaks ==="
if command -v gitleaks &> /dev/null; then
  gitleaks detect --source . -v
else
  echo "ERROR: gitleaks is not installed"
  echo "Install it from: https://github.com/gitleaks/gitleaks#installing"
  exit 1
fi

echo ""
echo "=== Checking for vulnerabilities ==="
pnpm audit --audit-level=moderate

echo ""
echo "=== Checking for outdated dependencies ==="
outdated_output=$(pnpm outdated 2>&1) || true
if echo "$outdated_output" | grep -q "│"; then
  echo "$outdated_output"
  echo ""
  echo "ERROR: Outdated dependencies found"
  exit 1
else
  echo "All dependencies are up to date"
fi

echo ""
echo "=== Health check passed ==="
