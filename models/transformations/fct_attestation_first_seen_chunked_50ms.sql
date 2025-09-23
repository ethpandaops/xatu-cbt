---
table: fct_attestation_first_seen_chunked_50ms
interval:
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - attestation
dependencies:
  - "{{transformation}}.int_attestation_first_seen"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Get attestation timing data from int_attestation_first_seen
WITH attestations AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        seen_slot_start_diff,
        attesting_validator_index
    FROM `{{ index .dep "{{transformation}}" "int_attestation_first_seen" "database" }}`.`int_attestation_first_seen` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND seen_slot_start_diff IS NOT NULL
),

-- Group attestations into 50ms chunks
attestations_chunked AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        floor(seen_slot_start_diff / 50) * 50 AS chunk_slot_start_diff,
        COUNT(DISTINCT attesting_validator_index) as attestation_count
    FROM attestations
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, chunk_slot_start_diff
),

-- Get all unique slot/block combinations from attestations
unique_slot_blocks AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root
    FROM attestations
),

-- Generate all possible chunk combinations for each slot/block (0ms to 12000ms in 50ms chunks)
slot_chunks AS (
    SELECT
        usb.slot,
        usb.slot_start_date_time,
        usb.epoch,
        usb.epoch_start_date_time,
        usb.block_root,
        arrayJoin(range(0, 12000, 50)) AS chunk_slot_start_diff
    FROM unique_slot_blocks usb
),

-- Join with actual attestation data
final_chunks AS (
    SELECT
        sc.slot,
        sc.slot_start_date_time,
        sc.epoch,
        sc.epoch_start_date_time,
        sc.block_root,
        sc.chunk_slot_start_diff,
        COALESCE(ac.attestation_count, 0) AS attestation_count
    FROM slot_chunks sc
    LEFT JOIN attestations_chunked ac
        ON sc.slot = ac.slot
        AND sc.slot_start_date_time = ac.slot_start_date_time
        AND sc.epoch = ac.epoch
        AND sc.epoch_start_date_time = ac.epoch_start_date_time
        AND sc.block_root = ac.block_root
        AND sc.chunk_slot_start_diff = ac.chunk_slot_start_diff
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    chunk_slot_start_diff,
    attestation_count
FROM final_chunks
WHERE attestation_count > 0 -- Only include chunks with actual attestations
SETTINGS join_use_nulls = 1
