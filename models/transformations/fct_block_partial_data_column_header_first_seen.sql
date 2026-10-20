---
table: fct_block_partial_data_column_header_first_seen
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - data_column
  - peerdas
  - partial_messages
dependencies:
  - "{{external}}.libp2p_gossipsub_partial_data_column"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Only Fulu partials carry the header, Gloas takes the commitments from the bid.
node_first_header AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root AS block_root,
        meta_client_name,
        min(propagation_slot_start_diff) AS node_seen_slot_start_diff,
        max(kzg_commitments_count) AS kzg_commitments_count,
        max(proposer_index) AS proposer_index
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_partial_data_column" "helpers" "from" }} FINAL
    WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
        AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND direction = 'RECV'
        AND has_header
        AND group_version = 0
        AND beacon_block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, meta_client_name
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    toUInt32(max(proposer_index)) AS proposer_index,
    toUInt32(max(kzg_commitments_count)) AS blob_count,
    toUInt32(min(node_seen_slot_start_diff)) AS seen_slot_start_diff,
    toUInt32(round(quantile(0.5)(node_seen_slot_start_diff))) AS p50_node_seen_slot_start_diff,
    toUInt32(round(quantile(0.9)(node_seen_slot_start_diff))) AS p90_node_seen_slot_start_diff,
    toUInt32(max(node_seen_slot_start_diff)) AS max_node_seen_slot_start_diff,
    toUInt32(count()) AS node_count,
    argMin(meta_client_name, node_seen_slot_start_diff) AS first_seen_meta_client_name
FROM node_first_header
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
