---
table: fct_data_column_partial_message_daily
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - data_column
  - peerdas
  - partial_messages
dependencies:
  # Either source is enough, as vantages emit partial parts and verdicts independently.
  - [
    "{{external}}.libp2p_gossipsub_partial_data_column",
    "{{external}}.libp2p_gossipsub_partial_data_column_outcome"
  ]
---
-- Rewrites every day the bounds touch so partial days are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toStartOfDay(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY AS max_day
),
parts AS (
    SELECT
        toDate(wallclock_slot_start_date_time) AS day_start_date,
        multiIf(
            position(meta_client_implementation, 'tysm') > 0, 'prysm',
            position(meta_client_implementation, 'temu') > 0, 'teku',
            position(meta_client_implementation, 'dimhouse') > 0, 'lighthouse',
            lower(extract(meta_client_implementation, '\\(([A-Za-z]+)\\)'))
        ) AS meta_consensus_implementation,
        toUInt32(uniqExact(meta_client_name)) AS node_count,
        toUInt32(countIf(direction = 'RECV' AND NOT has_message)) AS received_metadata_count,
        toUInt32(countIf(direction = 'RECV' AND has_message)) AS received_message_count,
        toUInt32(countIf(direction = 'RECV' AND cells_count > 0)) AS received_cell_message_count,
        toUInt64(sumIf(cells_count, direction = 'RECV')) AS received_cells,
        toUInt32(countIf(direction = 'RECV' AND has_header)) AS received_header_count,
        toUInt32(uniqExactIf(peer_id_unique_key, direction = 'RECV' AND cells_count > 0)) AS cell_sender_peer_count,
        toUInt32(countIf(direction = 'SEND' AND NOT has_message)) AS sent_metadata_count,
        toUInt32(countIf(direction = 'SEND' AND has_message)) AS sent_message_count,
        toUInt32(countIf(direction = 'SEND' AND cells_count > 0)) AS sent_cell_message_count,
        toUInt64(sumIf(cells_count, direction = 'SEND')) AS sent_cells,
        toUInt32(countIf(direction = 'SEND' AND has_header)) AS sent_header_count,
        toUInt32(0) AS cells_accept_count,
        toUInt32(0) AS cells_ignore_count,
        toUInt32(0) AS cells_reject_count,
        toUInt64(0) AS verdict_cells,
        toUInt64(0) AS new_cells,
        toUInt32(0) AS header_accept_count,
        toUInt32(0) AS header_ignore_count,
        toUInt32(0) AS header_reject_count,
        toUInt32(0) AS header_node_block_count,
        toUInt32(0) AS p50_header_seen_slot_start_diff,
        toUInt32(0) AS p90_header_seen_slot_start_diff
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_partial_data_column" "helpers" "from" }} FINAL
    WHERE wallclock_slot_start_date_time >= (SELECT min_day FROM day_bounds)
        AND wallclock_slot_start_date_time < (SELECT max_day FROM day_bounds)
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY day_start_date, meta_consensus_implementation
),
-- Only Fulu partials carry the header, Gloas takes the commitments from the bid.
node_headers AS (
    SELECT
        toDate(min(wallclock_slot_start_date_time)) AS day_start_date,
        multiIf(
            position(meta_client_implementation, 'tysm') > 0, 'prysm',
            position(meta_client_implementation, 'temu') > 0, 'teku',
            position(meta_client_implementation, 'dimhouse') > 0, 'lighthouse',
            lower(extract(meta_client_implementation, '\\(([A-Za-z]+)\\)'))
        ) AS meta_consensus_implementation,
        min(propagation_slot_start_diff) AS seen_slot_start_diff
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_partial_data_column" "helpers" "from" }} FINAL
    WHERE wallclock_slot_start_date_time >= (SELECT min_day FROM day_bounds)
        AND wallclock_slot_start_date_time < (SELECT max_day FROM day_bounds)
        AND direction = 'RECV'
        AND has_header
        AND group_version = 0
        AND beacon_block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY meta_consensus_implementation, meta_client_name, beacon_block_root
),
headers AS (
    SELECT
        day_start_date,
        meta_consensus_implementation,
        toUInt32(0) AS node_count,
        toUInt32(0) AS received_metadata_count,
        toUInt32(0) AS received_message_count,
        toUInt32(0) AS received_cell_message_count,
        toUInt64(0) AS received_cells,
        toUInt32(0) AS received_header_count,
        toUInt32(0) AS cell_sender_peer_count,
        toUInt32(0) AS sent_metadata_count,
        toUInt32(0) AS sent_message_count,
        toUInt32(0) AS sent_cell_message_count,
        toUInt64(0) AS sent_cells,
        toUInt32(0) AS sent_header_count,
        toUInt32(0) AS cells_accept_count,
        toUInt32(0) AS cells_ignore_count,
        toUInt32(0) AS cells_reject_count,
        toUInt64(0) AS verdict_cells,
        toUInt64(0) AS new_cells,
        toUInt32(0) AS header_accept_count,
        toUInt32(0) AS header_ignore_count,
        toUInt32(0) AS header_reject_count,
        toUInt32(count()) AS header_node_block_count,
        toUInt32(round(quantile(0.5)(seen_slot_start_diff))) AS p50_header_seen_slot_start_diff,
        toUInt32(round(quantile(0.9)(seen_slot_start_diff))) AS p90_header_seen_slot_start_diff
    FROM node_headers
    GROUP BY day_start_date, meta_consensus_implementation
),
verdicts AS (
    SELECT
        toDate(wallclock_slot_start_date_time) AS day_start_date,
        multiIf(
            position(meta_client_implementation, 'tysm') > 0, 'prysm',
            position(meta_client_implementation, 'temu') > 0, 'teku',
            position(meta_client_implementation, 'dimhouse') > 0, 'lighthouse',
            lower(extract(meta_client_implementation, '\\(([A-Za-z]+)\\)'))
        ) AS meta_consensus_implementation,
        toUInt32(0) AS node_count,
        toUInt32(0) AS received_metadata_count,
        toUInt32(0) AS received_message_count,
        toUInt32(0) AS received_cell_message_count,
        toUInt64(0) AS received_cells,
        toUInt32(0) AS received_header_count,
        toUInt32(0) AS cell_sender_peer_count,
        toUInt32(0) AS sent_metadata_count,
        toUInt32(0) AS sent_message_count,
        toUInt32(0) AS sent_cell_message_count,
        toUInt64(0) AS sent_cells,
        toUInt32(0) AS sent_header_count,
        toUInt32(countIf(stage = 'CELLS' AND outcome = 'ACCEPT')) AS cells_accept_count,
        toUInt32(countIf(stage = 'CELLS' AND outcome = 'IGNORE')) AS cells_ignore_count,
        toUInt32(countIf(stage = 'CELLS' AND outcome = 'REJECT')) AS cells_reject_count,
        toUInt64(sumIf(cells_count, stage = 'CELLS')) AS verdict_cells,
        toUInt64(sumIf(new_cells_count, stage = 'CELLS' AND outcome = 'ACCEPT')) AS new_cells,
        toUInt32(countIf(stage = 'HEADER' AND outcome = 'ACCEPT')) AS header_accept_count,
        toUInt32(countIf(stage = 'HEADER' AND outcome = 'IGNORE')) AS header_ignore_count,
        toUInt32(countIf(stage = 'HEADER' AND outcome = 'REJECT')) AS header_reject_count,
        toUInt32(0) AS header_node_block_count,
        toUInt32(0) AS p50_header_seen_slot_start_diff,
        toUInt32(0) AS p90_header_seen_slot_start_diff
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_partial_data_column_outcome" "helpers" "from" }} FINAL
    WHERE wallclock_slot_start_date_time >= (SELECT min_day FROM day_bounds)
        AND wallclock_slot_start_date_time < (SELECT max_day FROM day_bounds)
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY day_start_date, meta_consensus_implementation
),
combined AS (
    SELECT * FROM parts
    UNION ALL
    SELECT * FROM headers
    UNION ALL
    SELECT * FROM verdicts
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    meta_consensus_implementation,
    sum(node_count) AS node_count,
    sum(received_metadata_count) AS received_metadata_count,
    sum(received_message_count) AS received_message_count,
    sum(received_cell_message_count) AS received_cell_message_count,
    sum(received_cells) AS received_cells,
    sum(received_header_count) AS received_header_count,
    sum(cell_sender_peer_count) AS cell_sender_peer_count,
    sum(sent_metadata_count) AS sent_metadata_count,
    sum(sent_message_count) AS sent_message_count,
    sum(sent_cell_message_count) AS sent_cell_message_count,
    sum(sent_cells) AS sent_cells,
    sum(sent_header_count) AS sent_header_count,
    sum(cells_accept_count) AS cells_accept_count,
    sum(cells_ignore_count) AS cells_ignore_count,
    sum(cells_reject_count) AS cells_reject_count,
    sum(verdict_cells) AS verdict_cells,
    sum(new_cells) AS new_cells,
    sum(header_accept_count) AS header_accept_count,
    sum(header_ignore_count) AS header_ignore_count,
    sum(header_reject_count) AS header_reject_count,
    sum(header_node_block_count) AS header_node_block_count,
    max(p50_header_seen_slot_start_diff) AS p50_header_seen_slot_start_diff,
    max(p90_header_seen_slot_start_diff) AS p90_header_seen_slot_start_diff
FROM combined
GROUP BY day_start_date, meta_consensus_implementation
