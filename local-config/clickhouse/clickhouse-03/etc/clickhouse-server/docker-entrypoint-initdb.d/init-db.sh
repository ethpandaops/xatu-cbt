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

# Inject the per-replica password into the cluster definitions so inter-node
# distributed connections authenticate when CLICKHOUSE_PASSWORD is set. The shard/
# replica structure here MUST mirror config.d/config.xml (2 shards x 2 replicas for
# cluster_2S_2R, same host order) so ClickHouse's positional config merge lines the
# password up with the matching host+port already declared in config.xml.
cat <<EOT >> /etc/clickhouse-server/config.d/users.xml
<clickhouse replace="true">
    <remote_servers>
        <cluster_2S_2R>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>xatu-cbt-clickhouse-01</host>
                    $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password replace=\"true\">${CLICKHOUSE_PASSWORD}</password>")
                </replica>
                <replica>
                    <host>xatu-cbt-clickhouse-02</host>
                    $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password replace=\"true\">${CLICKHOUSE_PASSWORD}</password>")
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>xatu-cbt-clickhouse-03</host>
                    $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password replace=\"true\">${CLICKHOUSE_PASSWORD}</password>")
                </replica>
                <replica>
                    <host>xatu-cbt-clickhouse-04</host>
                    $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password replace=\"true\">${CLICKHOUSE_PASSWORD}</password>")
                </replica>
            </shard>
        </cluster_2S_2R>
        <xatu_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>xatu-clickhouse-01</host>
                    $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password replace=\"true\">${CLICKHOUSE_PASSWORD}</password>")
                </replica>
                <replica>
                    <host>xatu-clickhouse-02</host>
                    $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password replace=\"true\">${CLICKHOUSE_PASSWORD}</password>")
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>xatu-clickhouse-03</host>
                    $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password replace=\"true\">${CLICKHOUSE_PASSWORD}</password>")
                </replica>
                <replica>
                    <host>xatu-clickhouse-04</host>
                    $([ -n "${CLICKHOUSE_PASSWORD}" ] && echo "<password replace=\"true\">${CLICKHOUSE_PASSWORD}</password>")
                </replica>
            </shard>
        </xatu_cluster>

    </remote_servers>
</clickhouse>
EOT
