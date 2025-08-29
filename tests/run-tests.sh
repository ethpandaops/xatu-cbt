#!/bin/bash
set -e

# Configuration
TEST_PATTERN="${1:-*.test.yaml}"
DATA_SET="${2:-minimal}"
VERBOSE="${3:-false}"

# Generate unique test ID for database isolation
TEST_ID=$(uuidgen | tr '[:upper:]' '[:lower:]' | cut -c1-8)
export CBT_DATABASE_PREFIX="test_${TEST_ID}_"

echo "🧪 xatu-cbt Test Runner"
echo "━━━━━━━━━━━━━━━━━━━━━"
echo "📝 Test ID: $TEST_ID"
echo "🗄️  Database prefix: $CBT_DATABASE_PREFIX"

# Cleanup function
cleanup() {
  echo "🧹 Cleaning up test databases..."
  clickhouse-client -q "DROP DATABASE IF EXISTS ${CBT_DATABASE_PREFIX}mainnet"
  clickhouse-client -q "DROP DATABASE IF EXISTS ${CBT_DATABASE_PREFIX}admin"
}
trap cleanup EXIT

# 1. Bootstrap ClickHouse
echo "📦 Starting ClickHouse..."
(cd vendor/xatu && docker compose --profile clickhouse up -d)

# 2. Wait for ClickHouse health
echo "⏳ Waiting for ClickHouse..."
until curl -s "http://localhost:8123/ping" > /dev/null 2>&1; do
  sleep 2
done

# 3. Create isolated databases
echo "🏗️  Creating test databases..."
clickhouse-client -q "CREATE DATABASE IF NOT EXISTS ${CBT_DATABASE_PREFIX}mainnet"
clickhouse-client -q "CREATE DATABASE IF NOT EXISTS ${CBT_DATABASE_PREFIX}admin"

# 3a. Run xatu migrations to create base tables in isolated databases
echo "🔧 Running xatu migrations..."
docker run --rm \
  --network xatu_xatu-net \
  -v $(pwd)/vendor/xatu/deploy/migrations/clickhouse:/migrations \
  migrate/migrate \
  -path=/migrations \
  -database "clickhouse://xatu-clickhouse-01:9000?database=${CBT_DATABASE_PREFIX}mainnet&x-migrations-table=schema_migrations_xatu" \
  up

# 4. Load test data into isolated database
echo "📥 Loading data set: $DATA_SET"
go run tests/framework/loader.go \
  --data-set "tests/data-sets/$DATA_SET.yaml" \
  --database-prefix "$CBT_DATABASE_PREFIX"

# 5. Run CBT migrations with isolation
echo "🔄 Running migrations..."
docker compose -f docker-compose.test.yml run --rm \
  -e CBT_DATABASE_PREFIX="$CBT_DATABASE_PREFIX" \
  cbt-migrator

# 6. Start CBT engine with isolation
echo "🚀 Starting CBT engine..."
docker compose -f docker-compose.test.yml run -d \
  -e CBT_DATABASE_PREFIX="$CBT_DATABASE_PREFIX" \
  --name "cbt-engine-$TEST_ID" \
  cbt-engine

# 7. Wait for transformations
echo "⏳ Processing transformations..."
go run tests/framework/runner.go \
  --pattern "$TEST_PATTERN" \
  --timeout 300 \
  --database-prefix "$CBT_DATABASE_PREFIX"

# 8. Run assertions
echo "✅ Running assertions..."
go run tests/framework/assertions.go \
  --pattern "$TEST_PATTERN" \
  --database-prefix "$CBT_DATABASE_PREFIX"

echo "━━━━━━━━━━━━━━━━━━━━━"
echo "✨ All tests passed!"