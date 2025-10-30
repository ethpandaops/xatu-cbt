---
table: fct_data_column_availability_by_slot
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - data_column
  - peerdas
  - custody
dependencies:
  - "{{external}}.libp2p_rpc_data_column_custody_probe"
  - "{{external}}.libp2p_gossipsub_data_column_sidecar"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH combined_sources AS (
    -- Active custody probes (explicit results)
    -- These are direct RPC probes checking if peers have custody of specific columns
    SELECT
        'custody_probe' AS source,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        wallclock_request_slot,
        wallclock_request_slot_start_date_time,
        wallclock_request_epoch,
        wallclock_request_epoch_start_date_time,
        column_index,
        column_rows_count AS blob_count_raw,
        beacon_block_root,
        result,
        response_time_ms,
        peer_id_unique_key,
        meta_client_name,
        meta_client_implementation
    FROM {{ index .dep "{{external}}" "libp2p_rpc_data_column_custody_probe" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
      AND meta_network_name = '{{ .env.NETWORK }}'

    UNION ALL

    -- Passive gossipsub observations (implicit success)
    -- When a peer propagates a column on gossipsub, it implies they have custody of it
    SELECT
        'gossipsub' AS source,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        wallclock_slot AS wallclock_request_slot,
        wallclock_slot_start_date_time AS wallclock_request_slot_start_date_time,
        wallclock_epoch AS wallclock_request_epoch,
        wallclock_epoch_start_date_time AS wallclock_request_epoch_start_date_time,
        column_index,
        kzg_commitments_count AS blob_count_raw,
        beacon_block_root,
        'success' AS result,  -- Gossipsub propagation implies successful custody
        propagation_slot_start_diff AS response_time_ms,  -- Use propagation delay as proxy
        peer_id_unique_key,
        meta_client_name,
        meta_client_implementation
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_data_column_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
      AND meta_network_name = '{{ .env.NETWORK }}'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    -- Use argMin to pick wallclock values from earliest observation
    -- This allows custody probe and gossipsub to combine properly
    argMin(wallclock_request_slot, response_time_ms) AS wallclock_request_slot,
    argMin(wallclock_request_slot_start_date_time, response_time_ms) AS wallclock_request_slot_start_date_time,
    argMin(wallclock_request_epoch, response_time_ms) AS wallclock_request_epoch,
    argMin(wallclock_request_epoch_start_date_time, response_time_ms) AS wallclock_request_epoch_start_date_time,
    column_index,
    -- Pick beacon_block_root from earliest observation (handles reorgs/forks)
    argMin(beacon_block_root, response_time_ms) AS beacon_block_root,
    -- Track number of unique block roots (>1 indicates reorg/fork)
    uniqExact(combined_sources.beacon_block_root) AS beacon_block_root_variants,
    max(blob_count_raw) AS blob_count,
    countIf(result = 'success') AS success_count,
    countIf(result = 'failure') AS failure_count,
    countIf(result = 'missing') AS missing_count,
    count(*) AS probe_count,
    round((success_count * 100.0 / probe_count), 2) AS availability_pct,
    if(success_count > 0, round(minIf(response_time_ms, result = 'success')), 0) AS min_response_time_ms,
    if(success_count > 0, round(quantileIf(0.50)(response_time_ms, result = 'success')), 0) AS p50_response_time_ms,
    if(success_count > 0, round(quantileIf(0.95)(response_time_ms, result = 'success')), 0) AS p95_response_time_ms,
    if(success_count > 0, round(quantileIf(0.99)(response_time_ms, result = 'success')), 0) AS p99_response_time_ms,
    if(success_count > 0, round(maxIf(response_time_ms, result = 'success')), 0) AS max_response_time_ms,
    countDistinct(peer_id_unique_key) AS unique_peer_count,
    countDistinct(meta_client_name) AS unique_client_count,
    countDistinct(meta_client_implementation) AS unique_implementation_count,
    -- Source attribution for observability and debugging
    countIf(source = 'custody_probe') AS custody_probe_count,
    countIf(source = 'gossipsub') AS gossipsub_count
FROM combined_sources
GROUP BY
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    column_index
