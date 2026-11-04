---
table: int_block_blob_count_canonical
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - blob
  - canonical
dependencies:
  - "{{external}}.canonical_beacon_blob_sidecar"
  - "{{external}}.canonical_beacon_block"
---
-- block_root comes from canonical_beacon_block, not the sidecars: xatu cannon before v1.4.9 wrote the
-- block body root into canonical_beacon_blob_sidecar.block_root, so on mainnet every slot from Deneb
-- to 12662431 carries the body root there.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH blobs AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        max(blob_index) + 1 AS blob_count
    FROM {{ index .dep "{{external}}" "canonical_beacon_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time
),
blocks AS (
    SELECT DISTINCT
        slot_start_date_time,
        block_root
    FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    bl.slot AS slot,
    bl.slot_start_date_time AS slot_start_date_time,
    bl.epoch AS epoch,
    bl.epoch_start_date_time AS epoch_start_date_time,
    b.block_root AS block_root,
    bl.blob_count AS blob_count
FROM blobs bl
GLOBAL INNER JOIN blocks b ON bl.slot_start_date_time = b.slot_start_date_time;
