---
table: int_storage_slot_lifecycle
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
  - "{{transformation}}.int_storage_slot_lifecycle_boundary"
  - "{{transformation}}.int_storage_slot_next_touch"
  - "{{transformation}}.int_storage_slot_diff_by_address_slot"
---
-- Aggregates touch statistics per lifecycle using boundaries from
-- int_storage_slot_lifecycle_boundary. Uses LAG for interval computation
-- and GROUP BY for aggregation. Merges batch stats with previous stats
-- for lifecycles that span multiple batches.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- NO FINAL: only key columns needed, GROUP BY collapses duplicates
batch_touches AS (
    SELECT block_number, address, slot_key
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_next_touch" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY block_number, address, slot_key
),
touched_slots AS (
    SELECT DISTINCT address, slot_key
    FROM batch_touches
),
-- NO FINAL: deduplicates via argMax on version column
batch_diffs AS (
    SELECT
        block_number, address, slot_key,
        argMax(effective_bytes_to, updated_date_time) as effective_bytes_to
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff_by_address_slot" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY block_number, address, slot_key
),
-- Lifecycle boundaries from Model A (argMax dedup)
boundaries AS (
    SELECT
        address,
        slot_key,
        lifecycle_number,
        argMax(birth_block, updated_date_time) as birth_block,
        argMax(death_block, updated_date_time) as death_block,
        argMax(effective_bytes_birth, updated_date_time) as effective_bytes_birth,
        argMax(effective_bytes_death, updated_date_time) as effective_bytes_death
    FROM `{{ .self.database }}`.int_storage_slot_lifecycle_boundary
    WHERE (address, slot_key) IN (SELECT address, slot_key FROM touched_slots)
    GROUP BY address, slot_key, lifecycle_number
    HAVING birth_block <= {{ .bounds.end }}
        AND (death_block IS NULL OR death_block >= {{ .bounds.start }})
),
-- Self-query: previous stats for continuing lifecycles
prev_stats AS (
    SELECT
        address,
        slot_key,
        lifecycle_number,
        argMax(touch_count, updated_date_time) as touch_count,
        argMax(effective_bytes_peak, updated_date_time) as effective_bytes_peak,
        argMax(last_touch_block, updated_date_time) as last_touch_block,
        argMax(interval_count, updated_date_time) as interval_count,
        argMax(interval_sum, updated_date_time) as interval_sum,
        argMax(interval_max, updated_date_time) as interval_max
    FROM `{{ .self.database }}`.`{{ .self.table }}`
    WHERE (address, slot_key) IN (SELECT address, slot_key FROM touched_slots)
    GROUP BY address, slot_key, lifecycle_number
),
-- Join touches with diffs + boundaries + prev_stats
touches_enriched AS (
    SELECT
        t.block_number,
        t.address,
        t.slot_key,
        d.effective_bytes_to,
        b.lifecycle_number,
        b.birth_block,
        b.death_block,
        b.effective_bytes_birth,
        b.effective_bytes_death,
        ifNull(ps.touch_count, toUInt32(0)) as prev_tc,
        ifNull(ps.effective_bytes_peak, toUInt8(0)) as prev_eb_peak,
        ifNull(ps.last_touch_block, toUInt32(0)) as prev_ltb,
        ifNull(ps.interval_count, toUInt32(0)) as prev_ic,
        ifNull(ps.interval_sum, toUInt64(0)) as prev_isum,
        ifNull(ps.interval_max, toUInt32(0)) as prev_imax
    FROM batch_touches t
    LEFT JOIN batch_diffs d
        ON t.block_number = d.block_number
        AND t.address = d.address
        AND t.slot_key = d.slot_key
    INNER JOIN boundaries b
        ON t.address = b.address
        AND t.slot_key = b.slot_key
        AND t.block_number >= b.birth_block
        AND (b.death_block IS NULL OR t.block_number <= b.death_block)
    LEFT JOIN prev_stats ps
        ON b.address = ps.address
        AND b.slot_key = ps.slot_key
        AND b.lifecycle_number = ps.lifecycle_number
),
-- Compute touch-to-touch intervals via LAG window function
touches_with_intervals AS (
    SELECT
        *,
        COALESCE(
            nullIf(
                toUInt32(
                    block_number - lagInFrame(block_number, 1, block_number) OVER (
                        PARTITION BY address, slot_key, lifecycle_number
                        ORDER BY block_number
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                ),
                0
            ),
            if(prev_ltb > 0 AND block_number > prev_ltb,
                toUInt32(block_number - prev_ltb),
                NULL
            )
        ) as touch_interval
    FROM touches_enriched
),
-- Aggregate batch stats per lifecycle
batch_agg AS (
    SELECT
        address,
        slot_key,
        lifecycle_number,
        any(birth_block) as birth_block,
        any(death_block) as death_block,
        any(effective_bytes_birth) as effective_bytes_birth,
        any(effective_bytes_death) as effective_bytes_death,
        any(prev_tc) as prev_tc,
        any(prev_eb_peak) as prev_eb_peak,
        any(prev_ic) as prev_ic,
        any(prev_isum) as prev_isum,
        any(prev_imax) as prev_imax,
        toUInt32(COUNT(*)) as batch_touch_count,
        MAX(effective_bytes_to) as batch_eb_peak,
        MAX(block_number) as batch_last_touch,
        toUInt32(countIf(touch_interval IS NOT NULL)) as batch_interval_count,
        ifNull(
            toUInt64(sumIf(touch_interval, touch_interval IS NOT NULL)),
            toUInt64(0)
        ) as batch_interval_sum,
        ifNull(
            toUInt32(maxIf(touch_interval, touch_interval IS NOT NULL)),
            toUInt32(0)
        ) as batch_interval_max
    FROM touches_with_intervals
    GROUP BY address, slot_key, lifecycle_number
)
-- Merge batch stats with previous stats for continuing lifecycles
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    address,
    slot_key,
    lifecycle_number,
    birth_block,
    death_block,
    death_block - birth_block as lifespan_blocks,
    toUInt32(prev_tc + batch_touch_count) as touch_count,
    effective_bytes_birth,
    greatest(
        prev_eb_peak,
        ifNull(batch_eb_peak, toUInt8(0))
    ) as effective_bytes_peak,
    effective_bytes_death,
    batch_last_touch as last_touch_block,
    toUInt32(prev_ic + batch_interval_count) as interval_count,
    prev_isum + batch_interval_sum as interval_sum,
    greatest(prev_imax, batch_interval_max) as interval_max
FROM batch_agg
SETTINGS
    max_bytes_before_external_sort = 10000000000,
    max_bytes_before_external_group_by = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1,
    join_algorithm = 'parallel_hash';
