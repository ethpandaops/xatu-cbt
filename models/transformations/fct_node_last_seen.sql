---
table: fct_node_last_seen
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - xatu
  - nodes
dependencies:
  - "{{transformation}}.fct_block_first_seen_by_node"
---
-- The latest block observation of every Xatu node with its identity, client and location at that time, the
-- all-history counterpart of fct_node_active_last_24h. Each run writes the latest observation in its bounds and
-- the table keeps the row with the latest last_seen_date_time per node, so runs in any order converge without
-- reading the table back.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    max(slot_start_date_time) AS last_seen_date_time,
    argMax(username, slot_start_date_time) AS username,
    argMax(node_id, slot_start_date_time) AS node_id,
    argMax(classification, slot_start_date_time) AS classification,
    meta_client_name,
    argMax(meta_client_version, slot_start_date_time) AS meta_client_version,
    argMax(meta_client_implementation, slot_start_date_time) AS meta_client_implementation,
    argMax(meta_client_geo_city, slot_start_date_time) AS meta_client_geo_city,
    argMax(meta_client_geo_country, slot_start_date_time) AS meta_client_geo_country,
    argMax(meta_client_geo_country_code, slot_start_date_time) AS meta_client_geo_country_code,
    argMax(meta_client_geo_continent_code, slot_start_date_time) AS meta_client_geo_continent_code,
    argMax(meta_client_geo_longitude, slot_start_date_time) AS meta_client_geo_longitude,
    argMax(meta_client_geo_latitude, slot_start_date_time) AS meta_client_geo_latitude,
    argMax(meta_client_geo_autonomous_system_number, slot_start_date_time) AS meta_client_geo_autonomous_system_number,
    argMax(meta_client_geo_autonomous_system_organization, slot_start_date_time) AS meta_client_geo_autonomous_system_organization,
    argMax(meta_consensus_version, slot_start_date_time) AS meta_consensus_version,
    argMax(meta_consensus_implementation, slot_start_date_time) AS meta_consensus_implementation
FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY meta_client_name
-- One row per node across shards, so the insert routes each node to its own shard.
SETTINGS parallel_distributed_insert_select = 0
