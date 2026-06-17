#!/bin/bash
set -e
cat /etc/clickhouse-server/users.d/users.xml

cat <<EOT >> /etc/clickhouse-server/users.d/default.xml
<yandex>
  <users>
    <${CLICKHOUSE_USER}>
      <profile>default</profile>
      <networks>
        <ip>::/0</ip>
      </networks>
      $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password>${CLICKHOUSE_PASSWORD}</password>")
      <quota>default</quota>
    </${CLICKHOUSE_USER}>
    <readonly>
      <password>${CLICKHOUSE_USER_READONLY_PASSWORD}</password>
    </readonly>
  </users>
</yandex>
EOT

# This script configures users only. Migration state is owned by golang-migrate:
# each xatu set (xatu/observoor/admin) tracks its own schema_migrations_<set>
# table in its target database, and the target databases are created out-of-band
# (by the migration runner) before migrations run.

echo "ClickHouse node initialized"