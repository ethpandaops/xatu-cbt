---
table: fct_sync_committee_participation_by_validator_daily
type: incremental
interval:
  type: slot
  max: 100000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 10m"
tags:
  - daily
  - sync_committee
  - canonical
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_block_sync_aggregate"
---
-- This query expands the slot range to complete day boundaries to handle partial
-- day aggregations at the head of incremental processing. The ReplacingMergeTree
-- will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the day boundaries for the current slot range
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_sync_aggregate" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),

    -- All sync committee participation data within the day boundaries
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
            WHERE slot_start_date_time >= toDateTime((SELECT min_day FROM day_bounds))
              AND slot_start_date_time < toDateTime((SELECT max_day FROM day_bounds)) + INTERVAL 1 DAY
              AND meta_network_name = '{{ .env.NETWORK }}'

            UNION ALL

            -- Validators that missed
            SELECT
                slot,
                slot_start_date_time,
                arrayJoin(validators_missed) AS validator_index,
                false AS participated
            FROM {{ index .dep "{{external}}" "canonical_beacon_block_sync_aggregate" "helpers" "from" }} FINAL
            WHERE slot_start_date_time >= toDateTime((SELECT min_day FROM day_bounds))
              AND slot_start_date_time < toDateTime((SELECT max_day FROM day_bounds)) + INTERVAL 1 DAY
              AND meta_network_name = '{{ .env.NETWORK }}'
        )
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
    validator_index,
    -- Aggregated metrics per day
    toUInt32(count()) AS total_slots,
    toUInt32(countIf(participated)) AS participated_count,
    toUInt32(countIf(NOT participated)) AS missed_count
FROM slot_participation
GROUP BY day_start_date, validator_index
