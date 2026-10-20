---
table: fct_block_data_column_completion
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
  - "{{external}}.libp2p_data_column_completed"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
node_completions AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root AS block_root,
        meta_client_name,
        max(kzg_commitments_count) AS blob_count,
        uniqExactState(column_index) AS column_state,
        count() AS completion_count,
        countIf(completed_via = 'FULL_GOSSIP') AS full_gossip_completion_count,
        countIf(completed_via = 'PARTIAL_GOSSIP') AS partial_gossip_completion_count,
        countIf(completed_via = 'EL') AS el_completion_count,
        countIf(completed_via = 'RECONSTRUCTION') AS reconstruction_completion_count,
        countIf(completed_via NOT IN ('FULL_GOSSIP', 'PARTIAL_GOSSIP', 'EL', 'RECONSTRUCTION')) AS unknown_completion_count,
        sum(cells_total) AS cells_total,
        sum(cells_from_gossip) AS cells_from_gossip,
        sum(cells_from_el) AS cells_from_el,
        min(propagation_slot_start_diff) AS min_completion_slot_start_diff,
        quantilesState(0.5, 0.9)(propagation_slot_start_diff) AS completion_quantiles_state,
        max(propagation_slot_start_diff) AS last_completion_slot_start_diff
    FROM {{ index .dep "{{external}}" "libp2p_data_column_completed" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
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
    toUInt32(max(blob_count)) AS blob_count,
    toUInt32(uniqExactMerge(column_state)) AS column_count,
    toUInt32(count()) AS node_count,
    toUInt32(sum(completion_count)) AS completion_count,
    toUInt32(sum(full_gossip_completion_count)) AS full_gossip_completion_count,
    toUInt32(sum(partial_gossip_completion_count)) AS partial_gossip_completion_count,
    toUInt32(sum(el_completion_count)) AS el_completion_count,
    toUInt32(sum(reconstruction_completion_count)) AS reconstruction_completion_count,
    toUInt32(sum(unknown_completion_count)) AS unknown_completion_count,
    toUInt64(sum(cells_total)) AS cells_total,
    toUInt64(sum(cells_from_gossip)) AS cells_from_gossip,
    toUInt64(sum(cells_from_el)) AS cells_from_el,
    toUInt32(min(min_completion_slot_start_diff)) AS min_completion_slot_start_diff,
    toUInt32(round(quantilesMerge(0.5, 0.9)(completion_quantiles_state)[1])) AS p50_completion_slot_start_diff,
    toUInt32(round(quantilesMerge(0.5, 0.9)(completion_quantiles_state)[2])) AS p90_completion_slot_start_diff,
    toUInt32(max(last_completion_slot_start_diff)) AS max_completion_slot_start_diff,
    toUInt32(round(quantile(0.5)(last_completion_slot_start_diff))) AS p50_node_last_completion_slot_start_diff,
    toUInt32(round(quantile(0.9)(last_completion_slot_start_diff))) AS p90_node_last_completion_slot_start_diff
FROM node_completions
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
