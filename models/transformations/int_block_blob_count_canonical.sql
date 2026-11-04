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
-- One row per canonical block that carries blobs. The count comes from the block itself: its execution payload's
-- blob gas used divided by GAS_PER_BLOB (2^17), which is exact for every fork up to Gloas. Blob sidecars are only
-- a fallback for blocks without an execution payload in the beacon block (Gloas onwards), because cannon can miss
-- them and beacon nodes stop serving them after about 4096 epochs, so they can't be backfilled.
-- block_root comes from canonical_beacon_block, not the sidecars: xatu cannon before v1.4.9 wrote the
-- block body root into canonical_beacon_blob_sidecar.block_root, so on mainnet every slot from Deneb
-- to 12662431 carries the body root there.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        max(execution_payload_blob_gas_used) AS blob_gas_used
    FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
),
sidecars AS (
    SELECT
        slot_start_date_time,
        max(blob_index) + 1 AS blob_count
    FROM {{ index .dep "{{external}}" "canonical_beacon_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot_start_date_time
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    blob_count
FROM (
    SELECT
        b.slot AS slot,
        b.slot_start_date_time AS slot_start_date_time,
        b.epoch AS epoch,
        b.epoch_start_date_time AS epoch_start_date_time,
        b.block_root AS block_root,
        toUInt32(coalesce(nullIf(intDiv(b.blob_gas_used, 131072), 0), s.blob_count, 0)) AS blob_count
    FROM blocks AS b
    GLOBAL LEFT JOIN sidecars AS s ON b.slot_start_date_time = s.slot_start_date_time
)
WHERE blob_count > 0
SETTINGS join_use_nulls = 1
