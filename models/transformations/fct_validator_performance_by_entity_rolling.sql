---
table: fct_validator_performance_by_entity_rolling
type: scheduled
schedule: "@every 30m"
tags:
  - attestation
  - vote_correctness
  - block
  - proposer
  - entity
dependencies:
  - "{{transformation}}.fct_attestation_vote_correctness_by_entity_hourly"
  - "{{transformation}}.fct_attestation_vote_correctness_by_entity_daily"
  - "{{transformation}}.fct_block_proposer_by_entity"
  - "{{transformation}}.fct_block_proposal_by_entity_daily"
  - "{{transformation}}.int_block_mev_canonical"
---
-- The newest hour and day of each source are still filling, so periods end where both sources have a later
-- hour or day. Day periods start no earlier than the first full day both sources hold.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            least(
                (SELECT max(hour_start_date_time) FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_entity_hourly" "helpers" "from" }}),
                (SELECT toStartOfHour(max(slot_start_date_time)) FROM {{ index .dep "{{transformation}}" "fct_block_proposer_by_entity" "helpers" "from" }})
            ) AS end_hour,
            end_hour - INTERVAL 24 HOUR AS start_hour
    ),
    day_bounds AS (
        SELECT
            least(
                (SELECT max(day_start_date) FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_entity_daily" "helpers" "from" }}),
                (SELECT max(day_start_date) FROM {{ index .dep "{{transformation}}" "fct_block_proposal_by_entity_daily" "helpers" "from" }})
            ) AS end_day,
            greatest(
                (SELECT min(day_start_date) FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_entity_daily" "helpers" "from" }}),
                (SELECT min(day_start_date) FROM {{ index .dep "{{transformation}}" "fct_block_proposal_by_entity_daily" "helpers" "from" }})
            ) + 1 AS first_day
    ),
    -- Every period with its bounds. Day periods are (name, days), and 0 days reaches back to the first day.
    -- CBT splits model SQL on semicolons, so comments must not contain one.
    periods AS (
        SELECT
            '24h' AS period,
            (SELECT start_hour FROM hour_bounds) AS period_start_date_time,
            (SELECT end_hour FROM hour_bounds) AS period_end_date_time

        UNION ALL

        SELECT
            day_period.1 AS period,
            toDateTime(if(day_period.2 = 0, first_day, greatest(first_day, end_day - day_period.2))) AS period_start_date_time,
            toDateTime(end_day) AS period_end_date_time
        FROM day_bounds
        ARRAY JOIN [('7d', 7), ('30d', 30), ('90d', 90), ('1y', 365), ('all', 0)] AS day_period
    ),
    mev_24h AS (
        SELECT block_root, toFloat64(value) / 1e18 AS value_eth
        FROM {{ index .dep "{{transformation}}" "int_block_mev_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT start_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT end_hour FROM hour_bounds)
    ),
    proposals_24h AS (
        SELECT p.entity AS entity, p.`status` AS `status`, m.block_root IS NOT NULL AS is_mev, m.value_eth AS value_eth
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer_by_entity" "helpers" "from" }} AS p FINAL
        GLOBAL LEFT JOIN mev_24h AS m ON p.block_root = m.block_root
        WHERE p.slot_start_date_time >= (SELECT start_hour FROM hour_bounds)
          AND p.slot_start_date_time < (SELECT end_hour FROM hour_bounds)
    ),
    -- One row per period and entity from each source, merged below
    measures AS (
        SELECT
            '24h' AS period,
            entity,
            sum(total_duties) AS total_duties,
            sum(attested_count) AS attested_count,
            sum(missed_count) AS missed_count,
            sum(head_correct_count) AS head_correct_count,
            sum(target_correct_count) AS target_correct_count,
            sum(source_correct_count) AS source_correct_count,
            sum(ifNull(s.avg_inclusion_distance * s.attested_count, 0)) AS inclusion_distance_sum,
            sum(if(s.avg_inclusion_distance IS NULL, 0, s.attested_count)) AS inclusion_attested_count,
            0 AS block_count,
            0 AS mev_block_count,
            0 AS mev_value_sum,
            0 AS mev_value_count,
            0 AS orphaned_block_count,
            0 AS missed_slot_count
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_entity_hourly" "helpers" "from" }} AS s FINAL
        WHERE hour_start_date_time >= (SELECT start_hour FROM hour_bounds)
          AND hour_start_date_time < (SELECT end_hour FROM hour_bounds)
        GROUP BY entity

        UNION ALL

        SELECT
            day_period.1 AS period,
            entity,
            sum(total_duties) AS total_duties,
            sum(attested_count) AS attested_count,
            sum(missed_count) AS missed_count,
            sum(head_correct_count) AS head_correct_count,
            sum(target_correct_count) AS target_correct_count,
            sum(source_correct_count) AS source_correct_count,
            sum(ifNull(s.avg_inclusion_distance * s.attested_count, 0)) AS inclusion_distance_sum,
            sum(if(s.avg_inclusion_distance IS NULL, 0, s.attested_count)) AS inclusion_attested_count,
            0 AS block_count,
            0 AS mev_block_count,
            0 AS mev_value_sum,
            0 AS mev_value_count,
            0 AS orphaned_block_count,
            0 AS missed_slot_count
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_entity_daily" "helpers" "from" }} AS s FINAL
        ARRAY JOIN [('7d', 7), ('30d', 30), ('90d', 90), ('1y', 365), ('all', 0)] AS day_period
        WHERE day_start_date < (SELECT end_day FROM day_bounds)
          AND day_start_date >= if(
              day_period.2 = 0,
              (SELECT first_day FROM day_bounds),
              greatest((SELECT first_day FROM day_bounds), (SELECT end_day FROM day_bounds) - day_period.2)
          )
        GROUP BY period, entity

        UNION ALL

        SELECT
            '24h' AS period,
            entity,
            0 AS total_duties,
            0 AS attested_count,
            0 AS missed_count,
            0 AS head_correct_count,
            0 AS target_correct_count,
            0 AS source_correct_count,
            0 AS inclusion_distance_sum,
            0 AS inclusion_attested_count,
            countIf(`status` = 'canonical') AS block_count,
            countIf(`status` = 'canonical' AND is_mev) AS mev_block_count,
            sumIf(value_eth, `status` = 'canonical' AND is_mev AND value_eth IS NOT NULL) AS mev_value_sum,
            countIf(`status` = 'canonical' AND is_mev AND value_eth IS NOT NULL) AS mev_value_count,
            countIf(`status` = 'orphaned') AS orphaned_block_count,
            countIf(`status` = 'missed') AS missed_slot_count
        FROM proposals_24h
        GROUP BY entity

        UNION ALL

        SELECT
            day_period.1 AS period,
            entity,
            0 AS total_duties,
            0 AS attested_count,
            0 AS missed_count,
            0 AS head_correct_count,
            0 AS target_correct_count,
            0 AS source_correct_count,
            0 AS inclusion_distance_sum,
            0 AS inclusion_attested_count,
            sum(block_count) AS block_count,
            sum(mev_block_count) AS mev_block_count,
            sum(ifNull(s.avg_mev_value_eth * s.mev_block_count, 0)) AS mev_value_sum,
            sum(if(s.avg_mev_value_eth IS NULL, 0, s.mev_block_count)) AS mev_value_count,
            sum(orphaned_block_count) AS orphaned_block_count,
            sum(missed_slot_count) AS missed_slot_count
        FROM {{ index .dep "{{transformation}}" "fct_block_proposal_by_entity_daily" "helpers" "from" }} AS s FINAL
        ARRAY JOIN [('7d', 7), ('30d', 30), ('90d', 90), ('1y', 365), ('all', 0)] AS day_period
        WHERE day_start_date < (SELECT end_day FROM day_bounds)
          AND day_start_date >= if(
              day_period.2 = 0,
              (SELECT first_day FROM day_bounds),
              greatest((SELECT first_day FROM day_bounds), (SELECT end_day FROM day_bounds) - day_period.2)
          )
        GROUP BY period, entity
    ),
    merged AS (
        SELECT
            period,
            entity,
            sum(total_duties) AS total_duties,
            sum(attested_count) AS attested_count,
            sum(missed_count) AS missed_count,
            sum(head_correct_count) AS head_correct_count,
            sum(target_correct_count) AS target_correct_count,
            sum(source_correct_count) AS source_correct_count,
            sum(inclusion_distance_sum) / nullIf(sum(inclusion_attested_count), 0) AS avg_inclusion_distance,
            sum(block_count) AS block_count,
            sum(mev_block_count) AS mev_block_count,
            sum(mev_value_sum) / nullIf(sum(mev_value_count), 0) AS avg_mev_value_eth,
            sum(orphaned_block_count) AS orphaned_block_count,
            sum(missed_slot_count) AS missed_slot_count
        FROM measures
        GROUP BY period, entity
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    m.period AS period,
    m.entity AS entity,
    p.period_start_date_time AS period_start_date_time,
    p.period_end_date_time AS period_end_date_time,
    toUInt64(m.total_duties) AS total_duties,
    toUInt64(m.attested_count) AS attested_count,
    toUInt64(m.missed_count) AS missed_count,
    toUInt64(m.head_correct_count) AS head_correct_count,
    toUInt64(m.target_correct_count) AS target_correct_count,
    toUInt64(m.source_correct_count) AS source_correct_count,
    m.avg_inclusion_distance AS avg_inclusion_distance,
    toUInt32(m.block_count) AS block_count,
    toUInt32(m.mev_block_count) AS mev_block_count,
    m.avg_mev_value_eth AS avg_mev_value_eth,
    toUInt32(m.orphaned_block_count) AS orphaned_block_count,
    toUInt32(m.missed_slot_count) AS missed_slot_count
FROM merged AS m
GLOBAL INNER JOIN periods AS p ON m.period = p.period
-- Merge sums across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
