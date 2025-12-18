#!/bin/bash
set -e

echo "Running tests against all databases..."
echo ""

TEST_POSTGRES=1 TEST_CLICKHOUSE=1 TEST_TRINO=1 pnpm test

echo ""
echo "All database tests passed!"
