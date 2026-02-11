---
table: fct_block_blob_count_head
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - blob
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_data_column_sidecar"
  - "{{external}}.beacon_api_eth_v1_events_blob_sidecar"
  - "{{external}}.libp2p_gossipsub_data_column_sidecar"
  - "{{external}}.libp2p_gossipsub_blob_sidecar"
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
        max(kzg_commitments_count) AS `blob_count`
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_data_column_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
      AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root

    UNION ALL

    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        max(blob_index) + 1 AS `blob_count`
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
      AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root

    UNION ALL

    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root AS block_root,
        max(kzg_commitments_count) AS `blob_count`
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_data_column_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root

    UNION ALL

    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root AS block_root,
        max(blob_index) + 1 AS `blob_count`
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root
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
