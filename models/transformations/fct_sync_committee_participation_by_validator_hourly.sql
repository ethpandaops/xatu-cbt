---
table: fct_sync_committee_participation_by_validator_hourly
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - hourly
  - sync_committee
  - canonical
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_block_sync_aggregate"
---
-- This query expands the slot range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. The ReplacingMergeTree
-- will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries for the current slot range
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_sync_aggregate" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),

    -- All sync committee participation data within the hour boundaries
    slot_participation AS (
        SELECT
            slot,
            slot_start_date_time,
            validator_index,
            participated
        FROM (
            -- Validators that participated
            SELECT
                slot,
                slot_start_date_time,
                arrayJoin(validators_participated) AS validator_index,
                true AS participated
            FROM {{ index .dep "{{external}}" "canonical_beacon_block_sync_aggregate" "helpers" "from" }} FINAL
            WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
              AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
              AND meta_network_name = '{{ .env.NETWORK }}'

            UNION ALL

            -- Validators that missed
            SELECT
                slot,
                slot_start_date_time,
                arrayJoin(validators_missed) AS validator_index,
                false AS participated
            FROM {{ index .dep "{{external}}" "canonical_beacon_block_sync_aggregate" "helpers" "from" }} FINAL
            WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
              AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
              AND meta_network_name = '{{ .env.NETWORK }}'
        )
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    validator_index,
    -- Aggregated metrics per hour
    toUInt32(count()) AS total_slots,
    toUInt32(countIf(participated)) AS participated_count,
    toUInt32(countIf(NOT participated)) AS missed_count
FROM slot_participation
GROUP BY hour_start_date_time, validator_index
