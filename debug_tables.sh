#!/bin/bash

echo "=== Checking ClickHouse Tables After Migration ==="
echo ""

echo "1. All databases:"
docker exec xatu-clickhouse-01 clickhouse-client -q "SHOW DATABASES"
echo ""

echo "2. Tables in 'default' database:"
docker exec xatu-clickhouse-01 clickhouse-client -q "SHOW TABLES FROM default" | head -20
echo ""

echo "3. Tables matching 'beacon_api_eth_v1_events%':"
docker exec xatu-clickhouse-01 clickhouse-client -q "SELECT database, name FROM system.tables WHERE name LIKE '%beacon_api_eth_v1_events%' ORDER BY database, name"
echo ""

echo "4. Specific check for beacon_api_eth_v1_events_block:"
docker exec xatu-clickhouse-01 clickhouse-client -q "SELECT database, name, engine FROM system.tables WHERE name = 'beacon_api_eth_v1_events_block'"
echo ""

echo "5. Migration logs (last 50 lines):"
docker logs xatu-clickhouse-migrator 2>&1 | tail -50