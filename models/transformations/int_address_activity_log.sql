---
table: int_address_activity_log
type: incremental
interval:
  type: block
  max: 10000
schedules:
  forwardfill: "@every 1m"
# this model requires to chronologically process the data
fill:
  direction: tail
  allow_gap_skipping: false
  buffer: 160
tags:
  - address
  - account
  - activity
  - lifespan
dependencies:
  - "{{external}}.canonical_execution_balance_diffs"
  - "{{external}}.canonical_execution_balance_reads"
  - "{{external}}.canonical_execution_contracts"
  - "{{external}}.canonical_execution_nonce_reads"
  - "{{external}}.canonical_execution_nonce_diffs"
  - "{{external}}.canonical_execution_storage_diffs"
  - "{{external}}.canonical_execution_storage_reads"
  - "{{transformation}}.dim_block"
---
WITH new_activities AS (
    -- Gather all distinct activities from canonical execution tables
    SELECT DISTINCT
        lower(address) as address,
        block_number
    FROM (
        SELECT address, block_number
        FROM {{ index .dep "{{external}}" "canonical_execution_nonce_reads" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
          AND meta_network_name = '{{ .env.NETWORK }}'

        UNION ALL

        SELECT address, block_number
        FROM {{ index .dep "{{external}}" "canonical_execution_nonce_diffs" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
          AND meta_network_name = '{{ .env.NETWORK }}'

        UNION ALL

        SELECT address, block_number
        FROM {{ index .dep "{{external}}" "canonical_execution_balance_diffs" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
          AND meta_network_name = '{{ .env.NETWORK }}'

        UNION ALL

        SELECT address, block_number
        FROM {{ index .dep "{{external}}" "canonical_execution_balance_reads" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
          AND meta_network_name = '{{ .env.NETWORK }}'

        UNION ALL

        SELECT address, block_number
        FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
          AND meta_network_name = '{{ .env.NETWORK }}'

        UNION ALL

        SELECT contract_address as address, block_number
        FROM {{ index .dep "{{external}}" "canonical_execution_storage_reads" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
          AND meta_network_name = '{{ .env.NETWORK }}'

        UNION ALL

        SELECT contract_address as address, block_number
        FROM {{ index .dep "{{external}}" "canonical_execution_contracts" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
          AND meta_network_name = '{{ .env.NETWORK }}'
    )
),
activities_with_timestamps AS (
    -- Join with dim_block_canonical to get timestamps
    SELECT
        a.address,
        a.block_number,
        b.slot_start_date_time
    FROM new_activities a
    INNER JOIN {{ index .dep "{{transformation}}" "dim_block" "helpers" "from" }} AS b FINAL
        ON a.block_number = b.block_number
),
previous_activity_lookup AS (
    -- Get the most recent activity BEFORE current block range for each address
    SELECT
        address,
        argMax(slot_start_date_time, block_number) as last_activity_time
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE address IN (SELECT DISTINCT address FROM activities_with_timestamps)
      AND block_number < {{ .bounds.start }}
    GROUP BY address
),
activities_with_gap_calculation AS (
    -- Calculate time since last activity using window function and previous activity lookup
    SELECT
        n.address,
        n.block_number,
        n.slot_start_date_time,
        coalesce(
            -- Try to use window function to look at previous row in current batch
            dateDiff('second',
                lagInFrame(n.slot_start_date_time, 1, p.last_activity_time) OVER (
                    PARTITION BY n.address
                    ORDER BY n.block_number
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ),
                n.slot_start_date_time
            ),
            -- Fall back to comparing with historical last activity
            if(p.last_activity_time IS NOT NULL,
                dateDiff('second', p.last_activity_time, n.slot_start_date_time),
                0  -- First activity ever for this address
            )
        ) as seconds_since_last_activity
    FROM activities_with_timestamps n
    LEFT JOIN previous_activity_lookup p ON n.address = p.address
)
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    address,
    block_number,
    slot_start_date_time,
    seconds_since_last_activity,
    -- Mark as new lifespan if first activity (0 seconds) or gap > 6 months (15552000 seconds = 180 days)
    if(seconds_since_last_activity = 0 OR seconds_since_last_activity > 15552000, 1, 0) as is_new_lifespan
FROM activities_with_gap_calculation;
