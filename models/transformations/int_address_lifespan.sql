---
table: int_address_lifespan
type: incremental
interval:
  type: block
  max: 50000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - address
  - account
  - lifespan
dependencies:
  - "{{transformation}}.int_address_activity_log"
---
WITH recent_active_addresses AS (
    -- Find addresses that had activity in current block range
    SELECT DISTINCT address
    FROM {{ index .dep "{{transformation}}" "int_address_activity_log" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
all_activities_for_active_addresses AS (
    -- Get ALL historical activities for these addresses (not just current range)
    -- This is needed to correctly compute lifespan_id using running sum
    SELECT
        address,
        block_number,
        slot_start_date_time,
        is_new_lifespan
    FROM {{ index .dep "{{transformation}}" "int_address_activity_log" "helpers" "from" }} FINAL
    WHERE address IN (SELECT address FROM recent_active_addresses)
),
activity_with_lifespan_id AS (
    -- Assign lifespan_id using running sum of is_new_lifespan flags
    -- Each time is_new_lifespan = 1, the running sum increments, creating a new lifespan group
    SELECT
        address,
        block_number,
        slot_start_date_time,
        is_new_lifespan,
        sum(is_new_lifespan) OVER (
            PARTITION BY address
            ORDER BY block_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as lifespan_id
    FROM all_activities_for_active_addresses
)
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    address,
    lifespan_id,
    min(block_number) AS start_block_number,
    max(block_number) AS end_block_number,
    min(slot_start_date_time) AS start_timestamp,
    max(slot_start_date_time) AS end_timestamp,
    max(block_number) AS last_activity_block,
    -- Mark as active if last activity within 6 months (15552000 seconds = 180 days)
    if(dateDiff('second', max(slot_start_date_time), now()) <= 15552000, 1, 0) AS is_active,
    count() AS activity_count
FROM activity_with_lifespan_id
GROUP BY address, lifespan_id;
