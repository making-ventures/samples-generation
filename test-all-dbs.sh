#!/bin/bash
set -e

echo "Running e2e tests against all databases..."
echo ""

TEST_POSTGRES=1 TEST_CLICKHOUSE=1 TEST_TRINO=1 pnpm test:e2e

echo ""
echo "All database e2e tests passed!"
