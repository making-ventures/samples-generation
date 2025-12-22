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
./renovate-check.sh

echo ""
echo "=== Health check passed ==="
