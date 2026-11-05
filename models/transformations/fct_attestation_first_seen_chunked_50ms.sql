---
table: fct_attestation_first_seen_chunked_50ms
type: incremental
interval:
  type: slot
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
    FROM {{ index .dep "{{transformation}}" "int_attestation_first_seen" "helpers" "from" }} FINAL
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

-- Chunks this table already holds for the interval. A re-run writes a zero row for any of them that no
-- longer has attestations (a vote whose first sighting moved to an earlier chunk, or a block root that
-- lost its votes), so the stale row is replaced rather than counted twice.
existing_chunks AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        chunk_slot_start_diff
    FROM `{{ .self.database }}`.`{{ .self.table }}`
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

final_chunks AS (
    SELECT
        COALESCE(ac.slot, ec.slot) AS slot,
        COALESCE(ac.slot_start_date_time, ec.slot_start_date_time) AS slot_start_date_time,
        COALESCE(ac.epoch, ec.epoch) AS epoch,
        COALESCE(ac.epoch_start_date_time, ec.epoch_start_date_time) AS epoch_start_date_time,
        COALESCE(ac.block_root, ec.block_root) AS block_root,
        COALESCE(ac.chunk_slot_start_diff, ec.chunk_slot_start_diff) AS chunk_slot_start_diff,
        COALESCE(ac.attestation_count, 0) AS attestation_count
    FROM attestations_chunked ac
    GLOBAL FULL OUTER JOIN existing_chunks ec
        ON ac.slot = ec.slot
        AND ac.block_root = ec.block_root
        AND ac.chunk_slot_start_diff = ec.chunk_slot_start_diff
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
SETTINGS join_use_nulls = 1
