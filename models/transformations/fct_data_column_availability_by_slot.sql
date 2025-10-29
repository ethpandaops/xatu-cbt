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
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    wallclock_request_slot,
    wallclock_request_slot_start_date_time,
    wallclock_request_epoch,
    wallclock_request_epoch_start_date_time,
    column_index,
    max(column_rows_count) AS blob_count,
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
    countDistinct(meta_client_implementation) AS unique_implementation_count
FROM {{ index .dep "{{external}}" "libp2p_rpc_data_column_custody_probe" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    wallclock_request_slot,
    wallclock_request_slot_start_date_time,
    wallclock_request_epoch,
    wallclock_request_epoch_start_date_time,
    column_index
