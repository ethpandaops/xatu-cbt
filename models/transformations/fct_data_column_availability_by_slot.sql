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
  - "{{transformation}}.int_custody_probe_order_by_slot"
  - "{{external}}.libp2p_gossipsub_data_column_sidecar"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH combined_sources AS (
    -- Active custody probes (explicit results)
    -- These are direct RPC probes checking if peers have custody of specific columns
    -- Using int_custody_probe_order_by_slot which is ordered by slot_start_date_time for better query performance
    SELECT
        'custody_probe' AS source,
        slot,
        slot_start_date_time,
        -- Calculate epoch from slot (32 slots per epoch)
        toUInt32(slot DIV 32) AS epoch,
        toDateTime(toUnixTimestamp(slot_start_date_time) - ((slot % 32) * 12)) AS epoch_start_date_time,
        -- Use slot values for wallclock (probe is at slot time)
        slot AS wallclock_request_slot,
        slot_start_date_time AS wallclock_request_slot_start_date_time,
        toUInt32(slot DIV 32) AS wallclock_request_epoch,
        toDateTime(toUnixTimestamp(slot_start_date_time) - ((slot % 32) * 12)) AS wallclock_request_epoch_start_date_time,
        -- Expand column_indices array to individual rows
        arrayJoin(column_indices) AS column_index,
        -- Use blob_submitters array length as blob count
        toUInt16(length(blob_submitters)) AS blob_count_raw,
        -- beacon_block_root not available in int_custody_probe_order_by_slot
        -- Using empty string - beacon_block_root tracking comes from gossipsub source
        '' AS beacon_block_root,
        result,
        response_time_ms,
        peer_id_unique_key,
        username AS meta_client_name,
        meta_client_implementation
    FROM {{ index .dep "{{transformation}}" "int_custody_probe_order_by_slot" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})

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
    -- Use argMin to pick wallclock values from earliest observation (by actual timestamp)
    -- This allows custody probe and gossipsub to combine properly
    -- All references are qualified with combined_sources. to avoid alias shadowing
    argMin(wallclock_request_slot, combined_sources.wallclock_request_slot_start_date_time) AS wallclock_request_slot,
    min(combined_sources.wallclock_request_slot_start_date_time) AS wallclock_request_slot_start_date_time,
    argMin(wallclock_request_epoch, combined_sources.wallclock_request_slot_start_date_time) AS wallclock_request_epoch,
    argMin(wallclock_request_epoch_start_date_time, combined_sources.wallclock_request_slot_start_date_time) AS wallclock_request_epoch_start_date_time,
    column_index,
    -- Pick beacon_block_root from earliest observation (handles reorgs/forks)
    -- Use argMinIf to exclude empty roots from custody_probe source
    argMinIf(beacon_block_root, combined_sources.wallclock_request_slot_start_date_time, beacon_block_root != '') AS beacon_block_root,
    -- Track number of unique block roots (>1 indicates reorg/fork)
    -- Exclude empty strings from variant count (custody_probe doesn't have beacon_block_root)
    uniqExactIf(combined_sources.beacon_block_root, combined_sources.beacon_block_root != '') AS beacon_block_root_variants,
    max(blob_count_raw) AS blob_count,
    countIf(result = 'success') AS success_count,
    countIf(result = 'failure') AS failure_count,
    countIf(result = 'missing') AS missing_count,
    count(*) AS probe_count,
    round((success_count * 100.0 / probe_count), 2) AS availability_pct,
    -- Response time metrics only from custody probes (RPC latency, not gossipsub propagation)
    -- Use if(isFinite(...), ..., 0) to safely handle NaN/Inf when no custody probes exist in group
    if(isFinite(round(minIf(response_time_ms, result = 'success' AND source = 'custody_probe'))), toUInt32(round(minIf(response_time_ms, result = 'success' AND source = 'custody_probe'))), 0) AS min_response_time_ms,
    if(isFinite(round(quantileIf(0.50)(response_time_ms, result = 'success' AND source = 'custody_probe'))), toUInt32(round(quantileIf(0.50)(response_time_ms, result = 'success' AND source = 'custody_probe'))), 0) AS p50_response_time_ms,
    if(isFinite(round(quantileIf(0.95)(response_time_ms, result = 'success' AND source = 'custody_probe'))), toUInt32(round(quantileIf(0.95)(response_time_ms, result = 'success' AND source = 'custody_probe'))), 0) AS p95_response_time_ms,
    if(isFinite(round(quantileIf(0.99)(response_time_ms, result = 'success' AND source = 'custody_probe'))), toUInt32(round(quantileIf(0.99)(response_time_ms, result = 'success' AND source = 'custody_probe'))), 0) AS p99_response_time_ms,
    if(isFinite(round(maxIf(response_time_ms, result = 'success' AND source = 'custody_probe'))), toUInt32(round(maxIf(response_time_ms, result = 'success' AND source = 'custody_probe'))), 0) AS max_response_time_ms,
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
