#!/bin/bash
set -e

echo "========================================"
echo "Running all checks"
echo "========================================"

echo ""
./check.sh

echo ""
./health.sh

echo ""
echo "========================================"
echo "All checks passed successfully"
echo "========================================"
