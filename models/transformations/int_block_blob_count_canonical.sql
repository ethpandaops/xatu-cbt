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
  - "{{external}}.canonical_beacon_block"
  # Gloas blocks carry their blob count in the execution payload bid, earlier blocks in their blob gas. The block is
  # in the group too, so the range follows the blocks: a Gloas devnet's pre-Gloas blocks have neither bids nor
  # sidecars, and networks before Gloas have no bids.
  - - "{{external}}.canonical_beacon_block"
    - "{{external}}.canonical_beacon_block_execution_payload_bid"
    - "{{external}}.canonical_beacon_blob_sidecar"
---
-- One row per canonical block that carries blobs. The count comes from the block itself: before Gloas its execution
-- payload blob gas used divided by GAS_PER_BLOB (2^17), from Gloas the blob KZG commitments in its execution payload
-- bid. Blob sidecars are a last fallback only, because cannon can miss them and beacon nodes stop serving them after
-- about 4096 epochs, so they can't be backfilled.
-- block_root comes from canonical_beacon_block, not the sidecars: xatu cannon before v1.4.9 wrote the
-- block body root into canonical_beacon_blob_sidecar.block_root, so on mainnet every slot from Deneb
-- to 12662431 carries the body root there.
--
-- The block is in the dependency group, so the range can run ahead of the bid table, which a separate cannon deriver
-- fills. A Gloas block counted before its bid lands would get no row, and nothing goes back for it. So this first
-- statement fails the task (CBT retries it) while the range holds a Gloas or later block newer than the newest bid.
SELECT throwIf(
    (
        SELECT max(slot_start_date_time)
        FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }}
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
            AND block_version NOT IN ('phase0', 'altair', 'bellatrix', 'capella', 'deneb', 'electra', 'fulu')
    ) > (
        SELECT max(slot_start_date_time)
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_payload_bid" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
    ),
    'canonical_beacon_block_execution_payload_bid has not reached the last Gloas block in this range yet'
);

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
bids AS (
    SELECT
        slot_start_date_time,
        toString(block_root) AS block_root,
        max(blob_kzg_commitment_count) AS blob_count
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_payload_bid" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot_start_date_time, block_root
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
        toUInt32(coalesce(nullIf(intDiv(b.blob_gas_used, 131072), 0), nullIf(bid.blob_count, 0), s.blob_count, 0)) AS blob_count
    FROM blocks AS b
    GLOBAL LEFT JOIN bids AS bid ON b.slot_start_date_time = bid.slot_start_date_time AND toString(b.block_root) = bid.block_root
    GLOBAL LEFT JOIN sidecars AS s ON b.slot_start_date_time = s.slot_start_date_time
)
WHERE blob_count > 0
SETTINGS join_use_nulls = 1
