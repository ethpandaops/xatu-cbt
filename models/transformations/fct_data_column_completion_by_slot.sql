---
table: fct_data_column_completion_by_slot
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - data_column
  - peerdas
  - partial_messages
dependencies:
  # Any one source is enough, as vantages emit completions, partial parts and verdicts independently.
  - [
    "{{external}}.libp2p_data_column_completed",
    "{{external}}.libp2p_gossipsub_partial_data_column",
    "{{external}}.libp2p_gossipsub_partial_data_column_outcome"
  ]
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
completions AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root AS block_root,
        column_index,
        toUInt32(max(kzg_commitments_count)) AS blob_count,
        toUInt32(count()) AS completion_node_count,
        toUInt32(countIf(completed_via = 'FULL_GOSSIP')) AS full_gossip_completion_count,
        toUInt32(countIf(completed_via = 'PARTIAL_GOSSIP')) AS partial_gossip_completion_count,
        toUInt32(countIf(completed_via = 'EL')) AS el_completion_count,
        toUInt32(countIf(completed_via = 'RECONSTRUCTION')) AS reconstruction_completion_count,
        toUInt32(countIf(completed_via NOT IN ('FULL_GOSSIP', 'PARTIAL_GOSSIP', 'EL', 'RECONSTRUCTION'))) AS unknown_completion_count,
        toUInt64(sum(cells_total)) AS cells_total,
        toUInt64(sum(cells_from_gossip)) AS cells_from_gossip,
        toUInt64(sum(cells_from_el)) AS cells_from_el,
        ifNotFinite(avgIf(contributing_peers, completed_via = 'PARTIAL_GOSSIP'), 0) AS avg_partial_contributing_peers,
        toUInt32(min(propagation_slot_start_diff)) AS min_completion_slot_start_diff,
        toUInt32(round(quantile(0.5)(propagation_slot_start_diff))) AS p50_completion_slot_start_diff,
        toUInt32(round(quantile(0.9)(propagation_slot_start_diff))) AS p90_completion_slot_start_diff,
        toUInt32(max(propagation_slot_start_diff)) AS max_completion_slot_start_diff,
        toUInt32(round(ifNotFinite(quantileIf(0.5)(propagation_slot_start_diff, completed_via = 'FULL_GOSSIP'), 0))) AS p50_full_gossip_completion_slot_start_diff,
        toUInt32(round(ifNotFinite(quantileIf(0.5)(propagation_slot_start_diff, completed_via = 'PARTIAL_GOSSIP'), 0))) AS p50_partial_gossip_completion_slot_start_diff,
        toUInt32(round(ifNotFinite(quantileIf(0.5)(propagation_slot_start_diff, completed_via = 'EL'), 0))) AS p50_el_completion_slot_start_diff,
        toUInt32(0) AS partial_received_message_count,
        toUInt64(0) AS partial_received_cells,
        toUInt32(0) AS partial_sender_peer_count,
        toUInt32(0) AS partial_sent_message_count,
        toUInt64(0) AS partial_sent_cells,
        toUInt32(0) AS partial_accept_verdict_count,
        toUInt32(0) AS partial_ignore_verdict_count,
        toUInt32(0) AS partial_reject_verdict_count,
        toUInt64(0) AS partial_new_cells
    FROM {{ index .dep "{{external}}" "libp2p_data_column_completed" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND beacon_block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, column_index
),
partial_parts AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root AS block_root,
        column_index,
        toUInt32(max(bitmap_length)) AS blob_count,
        toUInt32(0) AS completion_node_count,
        toUInt32(0) AS full_gossip_completion_count,
        toUInt32(0) AS partial_gossip_completion_count,
        toUInt32(0) AS el_completion_count,
        toUInt32(0) AS reconstruction_completion_count,
        toUInt32(0) AS unknown_completion_count,
        toUInt64(0) AS cells_total,
        toUInt64(0) AS cells_from_gossip,
        toUInt64(0) AS cells_from_el,
        toFloat64(0) AS avg_partial_contributing_peers,
        toUInt32(0) AS min_completion_slot_start_diff,
        toUInt32(0) AS p50_completion_slot_start_diff,
        toUInt32(0) AS p90_completion_slot_start_diff,
        toUInt32(0) AS max_completion_slot_start_diff,
        toUInt32(0) AS p50_full_gossip_completion_slot_start_diff,
        toUInt32(0) AS p50_partial_gossip_completion_slot_start_diff,
        toUInt32(0) AS p50_el_completion_slot_start_diff,
        toUInt32(countIf(direction = 'RECV' AND has_message)) AS partial_received_message_count,
        toUInt64(sumIf(cells_count, direction = 'RECV' AND has_message)) AS partial_received_cells,
        toUInt32(uniqExactIf(peer_id_unique_key, direction = 'RECV' AND has_message)) AS partial_sender_peer_count,
        toUInt32(countIf(direction = 'SEND' AND has_message)) AS partial_sent_message_count,
        toUInt64(sumIf(cells_count, direction = 'SEND' AND has_message)) AS partial_sent_cells,
        toUInt32(0) AS partial_accept_verdict_count,
        toUInt32(0) AS partial_ignore_verdict_count,
        toUInt32(0) AS partial_reject_verdict_count,
        toUInt64(0) AS partial_new_cells
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_partial_data_column" "helpers" "from" }} FINAL
    WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
        AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND beacon_block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, column_index
),
verdicts AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root AS block_root,
        column_index,
        toUInt32(0) AS blob_count,
        toUInt32(0) AS completion_node_count,
        toUInt32(0) AS full_gossip_completion_count,
        toUInt32(0) AS partial_gossip_completion_count,
        toUInt32(0) AS el_completion_count,
        toUInt32(0) AS reconstruction_completion_count,
        toUInt32(0) AS unknown_completion_count,
        toUInt64(0) AS cells_total,
        toUInt64(0) AS cells_from_gossip,
        toUInt64(0) AS cells_from_el,
        toFloat64(0) AS avg_partial_contributing_peers,
        toUInt32(0) AS min_completion_slot_start_diff,
        toUInt32(0) AS p50_completion_slot_start_diff,
        toUInt32(0) AS p90_completion_slot_start_diff,
        toUInt32(0) AS max_completion_slot_start_diff,
        toUInt32(0) AS p50_full_gossip_completion_slot_start_diff,
        toUInt32(0) AS p50_partial_gossip_completion_slot_start_diff,
        toUInt32(0) AS p50_el_completion_slot_start_diff,
        toUInt32(0) AS partial_received_message_count,
        toUInt64(0) AS partial_received_cells,
        toUInt32(0) AS partial_sender_peer_count,
        toUInt32(0) AS partial_sent_message_count,
        toUInt64(0) AS partial_sent_cells,
        toUInt32(countIf(outcome = 'ACCEPT')) AS partial_accept_verdict_count,
        toUInt32(countIf(outcome = 'IGNORE')) AS partial_ignore_verdict_count,
        toUInt32(countIf(outcome = 'REJECT')) AS partial_reject_verdict_count,
        toUInt64(sumIf(new_cells_count, stage = 'CELLS' AND outcome = 'ACCEPT')) AS partial_new_cells
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_partial_data_column_outcome" "helpers" "from" }} FINAL
    WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
        AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND beacon_block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, column_index
),
combined AS (
    SELECT * FROM completions
    UNION ALL
    SELECT * FROM partial_parts
    UNION ALL
    SELECT * FROM verdicts
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    column_index,
    max(blob_count) AS blob_count,
    sum(completion_node_count) AS completion_node_count,
    sum(full_gossip_completion_count) AS full_gossip_completion_count,
    sum(partial_gossip_completion_count) AS partial_gossip_completion_count,
    sum(el_completion_count) AS el_completion_count,
    sum(reconstruction_completion_count) AS reconstruction_completion_count,
    sum(unknown_completion_count) AS unknown_completion_count,
    sum(cells_total) AS cells_total,
    sum(cells_from_gossip) AS cells_from_gossip,
    sum(cells_from_el) AS cells_from_el,
    round(max(avg_partial_contributing_peers), 2) AS avg_partial_contributing_peers,
    max(min_completion_slot_start_diff) AS min_completion_slot_start_diff,
    max(p50_completion_slot_start_diff) AS p50_completion_slot_start_diff,
    max(p90_completion_slot_start_diff) AS p90_completion_slot_start_diff,
    max(max_completion_slot_start_diff) AS max_completion_slot_start_diff,
    max(p50_full_gossip_completion_slot_start_diff) AS p50_full_gossip_completion_slot_start_diff,
    max(p50_partial_gossip_completion_slot_start_diff) AS p50_partial_gossip_completion_slot_start_diff,
    max(p50_el_completion_slot_start_diff) AS p50_el_completion_slot_start_diff,
    sum(partial_received_message_count) AS partial_received_message_count,
    sum(partial_received_cells) AS partial_received_cells,
    sum(partial_sender_peer_count) AS partial_sender_peer_count,
    sum(partial_sent_message_count) AS partial_sent_message_count,
    sum(partial_sent_cells) AS partial_sent_cells,
    sum(partial_accept_verdict_count) AS partial_accept_verdict_count,
    sum(partial_ignore_verdict_count) AS partial_ignore_verdict_count,
    sum(partial_reject_verdict_count) AS partial_reject_verdict_count,
    sum(partial_new_cells) AS partial_new_cells
FROM combined
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, column_index
