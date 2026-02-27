---
table: int_storage_slot_lifecycle_boundary
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1m"
tags:
  - execution
  - storage
  - lifecycle
dependencies:
  - "{{transformation}}.int_storage_slot_diff_by_address_slot"
---
-- Detects lifecycle boundaries: births (0->non-zero) and deaths (non-zero->0).
-- Assigns lifecycle_number via cumulative SUM of births per slot.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- NO FINAL: deduplicates via argMax, filters births/deaths after dedup
batch_events AS (
    SELECT
        block_number,
        address,
        slot_key,
        effective_bytes_from,
        effective_bytes_to,
        toUInt8(effective_bytes_from = 0 AND effective_bytes_to > 0) as is_birth,
        toUInt8(effective_bytes_from > 0 AND effective_bytes_to = 0) as is_death
    FROM (
        SELECT
            block_number, address, slot_key,
            argMax(effective_bytes_from, updated_date_time) as effective_bytes_from,
            argMax(effective_bytes_to, updated_date_time) as effective_bytes_to
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff_by_address_slot" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        GROUP BY block_number, address, slot_key
    )
    WHERE (effective_bytes_from = 0 AND effective_bytes_to > 0)
       OR (effective_bytes_from > 0 AND effective_bytes_to = 0)
),
event_slots AS (
    SELECT DISTINCT address, slot_key FROM batch_events
),
-- Self-query: single-pass tuple argMax picks latest (lifecycle, version)
prev_state AS (
    SELECT
        address,
        slot_key,
        max(lifecycle_number) as lnum,
        argMax(birth_block, (lifecycle_number, updated_date_time)) as birth,
        argMax(effective_bytes_birth, (lifecycle_number, updated_date_time)) as eb_birth
    FROM `{{ .self.database }}`.`{{ .self.table }}`
    WHERE (address, slot_key) IN (SELECT address, slot_key FROM event_slots)
    GROUP BY address, slot_key
),
-- Assign lifecycle_number = prev_lnum + cumulative births in batch
events_numbered AS (
    SELECT
        e.block_number,
        e.address,
        e.slot_key,
        e.effective_bytes_from,
        e.effective_bytes_to,
        e.is_birth,
        e.is_death,
        ifNull(p.lnum, toUInt32(0)) + toUInt32(
            SUM(toUInt32(e.is_birth)) OVER (
                PARTITION BY e.address, e.slot_key
                ORDER BY e.block_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) as lifecycle_number,
        ifNull(p.birth, toUInt32(0)) as prev_birth,
        ifNull(p.eb_birth, toUInt8(0)) as prev_eb_birth
    FROM batch_events e
    LEFT JOIN prev_state p
        ON e.address = p.address AND e.slot_key = p.slot_key
),
-- Aggregate per lifecycle: one row with birth and death info
lifecycle_boundaries AS (
    SELECT
        address,
        slot_key,
        lifecycle_number,
        COALESCE(
            MIN(if(is_birth = 1, block_number, NULL)),
            any(prev_birth)
        ) as birth_block,
        MAX(if(is_death = 1, block_number, NULL)) as death_block,
        COALESCE(
            MIN(if(is_birth = 1, effective_bytes_to, NULL)),
            any(prev_eb_birth)
        ) as effective_bytes_birth,
        MAX(if(is_death = 1, effective_bytes_from, NULL)) as effective_bytes_death
    FROM events_numbered
    WHERE lifecycle_number > 0
    GROUP BY address, slot_key, lifecycle_number
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    address,
    slot_key,
    lifecycle_number,
    birth_block,
    death_block,
    effective_bytes_birth,
    effective_bytes_death
FROM lifecycle_boundaries
SETTINGS
    max_bytes_before_external_sort = 10000000000,
    max_bytes_before_external_group_by = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1,
    join_algorithm = 'parallel_hash';
