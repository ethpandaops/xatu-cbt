---
table: fct_data_column_availability_by_slot_blob
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - blob
  - data_column
  - peerdas
  - custody
dependencies:
  - "{{transformation}}.fct_data_column_availability_by_slot"
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
    blob_index,
    column_index,
    blob_count,
    availability_pct,
    success_count,
    failure_count,
    missing_count,
    probe_count,
    min_response_time_ms,
    p50_response_time_ms,
    p95_response_time_ms,
    p99_response_time_ms,
    max_response_time_ms,
    unique_peer_count,
    unique_client_count,
    unique_implementation_count
FROM (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        wallclock_request_slot,
        wallclock_request_slot_start_date_time,
        wallclock_request_epoch,
        wallclock_request_epoch_start_date_time,
        column_index,
        blob_count,
        availability_pct,
        success_count,
        failure_count,
        missing_count,
        probe_count,
        min_response_time_ms,
        p50_response_time_ms,
        p95_response_time_ms,
        p99_response_time_ms,
        max_response_time_ms,
        unique_peer_count,
        unique_client_count,
        unique_implementation_count
    FROM {{ index .dep "{{transformation}}" "fct_data_column_availability_by_slot" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
ARRAY JOIN range(toUInt16(blob_count)) AS blob_index
