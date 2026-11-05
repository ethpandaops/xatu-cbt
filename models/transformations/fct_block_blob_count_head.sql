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
  # At least one of these must be present
  - [
    "{{external}}.beacon_api_eth_v1_events_data_column_sidecar",
    "{{external}}.beacon_api_eth_v1_events_blob_sidecar",
    "{{external}}.libp2p_gossipsub_data_column_sidecar",
    "{{external}}.libp2p_gossipsub_blob_sidecar"
  ]
---
-- xatu clmimicry before v1.6.12 wrote gossipsub block roots without the 0x prefix, so the libp2p
-- roots are normalised to match the beacon API and canonical roots.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH combined_events AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        max(kzg_commitments_count) AS `blob_count`,
        1 AS from_beacon_api
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
        max(blob_index) + 1 AS `blob_count`,
        1 AS from_beacon_api
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
        if(startsWith(beacon_block_root, '0x'), toString(beacon_block_root), concat('0x', toStringCutToZero(beacon_block_root))) AS block_root,
        max(kzg_commitments_count) AS `blob_count`,
        0 AS from_beacon_api
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_data_column_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND beacon_block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root

    UNION ALL

    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        if(startsWith(beacon_block_root, '0x'), toString(beacon_block_root), concat('0x', toStringCutToZero(beacon_block_root))) AS block_root,
        max(blob_index) + 1 AS `blob_count`,
        0 AS from_beacon_api
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND beacon_block_root != ''
        AND meta_network_name = '{{ .env.NETWORK }}'
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
-- libp2p sentries record gossip before validating it, and some relays (gnode-beacon) re-encode
-- sidecars, which changes the block root and breaks the header signature. Keep a root only if a
-- beacon node accepted a sidecar for it; beacon nodes emit the event after validation.
HAVING max(from_beacon_api) = 1
