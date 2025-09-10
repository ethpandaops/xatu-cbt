---
table: int_block_blob_count_head
interval:
  max: 500000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - blob
dependencies:
  # At least one of these must be present
  - [
    "{{external}}.beacon_api_eth_v1_events_data_column_sidecar",
    "{{external}}.beacon_api_eth_v1_events_blob_sidecar",
    # add libp2p_gossipsub_data_column_sidecar/blob_sidecar when it has block_root column
    # "{{external}}.libp2p_gossipsub_data_column_sidecar",
    # "{{external}}.libp2p_gossipsub_blob_sidecar"
  ]
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH combined_events AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        max(length(kzg_commitments)) AS `blob_count`
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_events_data_column_sidecar" "database" }}`.`beacon_api_eth_v1_events_data_column_sidecar` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root

    UNION ALL

    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        max(blob_index) + 1 AS `blob_count`
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_events_data_column_sidecar" "database" }}`.`beacon_api_eth_v1_events_blob_sidecar` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    max(blob_count) AS blob_count
FROM combined_events
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
