CREATE TABLE fct_block_proposer_by_entity_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the scheduled proposer in dim_node, or unknown' CODEC(ZSTD(1)),
    `proposer_validator_index` UInt32 COMMENT 'The validator index of the scheduled proposer' CODEC(ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root, NULL when the slot was missed' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'canonical, orphaned (a block was seen but is not on the finalized chain) or missed (no block was seen)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Every slot of the finalized chain with the staking entity of its scheduled proposer and how the proposal ended: canonical, orphaned or missed. Validators map to their current entity in dim_node, and validators it does not name are unknown';

CREATE TABLE fct_block_proposer_by_entity ON CLUSTER '{cluster}' AS fct_block_proposer_by_entity_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_proposer_by_entity_local,
    cityHash64(`slot_start_date_time`)
);

ALTER TABLE fct_block_proposer_by_entity_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_entity
(
    SELECT *
    ORDER BY (`entity`, `slot_start_date_time`)
);

CREATE TABLE fct_block_proposal_by_entity_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the scheduled proposer in dim_node, or unknown' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Canonical blocks proposed' CODEC(ZSTD(1)),
    `mev_block_count` UInt32 COMMENT 'Canonical blocks delivered by an MEV relay, the rest were built locally' CODEC(ZSTD(1)),
    `avg_mev_value_eth` Nullable(Float64) COMMENT 'Mean relay-reported value of the MEV blocks in ETH, NULL when there were none' CODEC(ZSTD(1)),
    `orphaned_block_count` UInt32 COMMENT 'Blocks proposed that did not make the finalized chain' CODEC(ZSTD(1)),
    `missed_slot_count` UInt32 COMMENT 'Slots the entity was scheduled to propose where no block was seen' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `entity`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily block proposals per staking entity on the finalized chain: canonical blocks, MEV share and value, orphaned blocks and missed slots. Proposals due are block_count + orphaned_block_count + missed_slot_count. Validators map to their current entity in dim_node, and validators it does not name are unknown, so the rows of a day sum to the whole network';

CREATE TABLE fct_block_proposal_by_entity_daily ON CLUSTER '{cluster}' AS fct_block_proposal_by_entity_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_proposal_by_entity_daily_local,
    cityHash64(`day_start_date`, `entity`)
);

CREATE TABLE fct_attestation_vote_correctness_by_entity_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the validators in dim_node, or unknown' CODEC(ZSTD(1)),
    `validator_count` UInt32 COMMENT 'Validators of the entity with attestation duties in the hour' CODEC(ZSTD(1)),
    `total_duties` UInt64 COMMENT 'Attestation duties' CODEC(ZSTD(1)),
    `attested_count` UInt64 COMMENT 'Attestations included on the finalized chain' CODEC(ZSTD(1)),
    `missed_count` UInt64 COMMENT 'Attestations missed: duties with no attestation included on the finalized chain' CODEC(ZSTD(1)),
    `head_correct_count` UInt64 COMMENT 'Head votes that were correct' CODEC(ZSTD(1)),
    `target_correct_count` UInt64 COMMENT 'Target votes that were correct' CODEC(ZSTD(1)),
    `source_correct_count` UInt64 COMMENT 'Source votes that were correct, from attestations seen on gossip' CODEC(ZSTD(1)),
    `avg_inclusion_distance` Nullable(Float64) COMMENT 'Mean inclusion distance in slots per included attestation, NULL when there were none' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `entity`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly attestation liveness, vote correctness and inclusion per staking entity on the finalized chain. Duties are total_duties = attested_count + missed_count. Validators map to their current entity in dim_node, and validators it does not name are unknown, so the rows of an hour sum to the whole network';

CREATE TABLE fct_attestation_vote_correctness_by_entity_hourly ON CLUSTER '{cluster}' AS fct_attestation_vote_correctness_by_entity_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_attestation_vote_correctness_by_entity_hourly_local,
    cityHash64(`hour_start_date_time`, `entity`)
);

ALTER TABLE fct_attestation_vote_correctness_by_entity_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_entity
(
    SELECT *
    ORDER BY (`entity`, `hour_start_date_time`)
);

CREATE TABLE fct_attestation_vote_correctness_by_entity_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the validators in dim_node, or unknown' CODEC(ZSTD(1)),
    `validator_count` UInt32 COMMENT 'Most validators of the entity with attestation duties in one hour of the day' CODEC(ZSTD(1)),
    `total_duties` UInt64 COMMENT 'Attestation duties' CODEC(ZSTD(1)),
    `attested_count` UInt64 COMMENT 'Attestations included on the finalized chain' CODEC(ZSTD(1)),
    `missed_count` UInt64 COMMENT 'Attestations missed: duties with no attestation included on the finalized chain' CODEC(ZSTD(1)),
    `head_correct_count` UInt64 COMMENT 'Head votes that were correct' CODEC(ZSTD(1)),
    `target_correct_count` UInt64 COMMENT 'Target votes that were correct' CODEC(ZSTD(1)),
    `source_correct_count` UInt64 COMMENT 'Source votes that were correct, from attestations seen on gossip' CODEC(ZSTD(1)),
    `avg_inclusion_distance` Nullable(Float64) COMMENT 'Mean inclusion distance in slots per included attestation, NULL when there were none' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `entity`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily attestation liveness, vote correctness and inclusion per staking entity on the finalized chain. Duties are total_duties = attested_count + missed_count. Validators map to their current entity in dim_node, and validators it does not name are unknown, so the rows of a day sum to the whole network';

CREATE TABLE fct_attestation_vote_correctness_by_entity_daily ON CLUSTER '{cluster}' AS fct_attestation_vote_correctness_by_entity_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_attestation_vote_correctness_by_entity_daily_local,
    cityHash64(`day_start_date`, `entity`)
);

ALTER TABLE fct_attestation_vote_correctness_by_entity_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_entity
(
    SELECT *
    ORDER BY (`entity`, `day_start_date`)
);

CREATE TABLE fct_validator_performance_by_entity_rolling_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `period` LowCardinality(String) COMMENT 'Rolling period: 24h (the last 24 complete hours), 7d, 30d, 90d or 1y (the last complete UTC days), or all (every day with data)' CODEC(ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the validators in dim_node, or unknown' CODEC(ZSTD(1)),
    `period_start_date_time` DateTime COMMENT 'Start of the period, or of the data when it starts later' CODEC(ZSTD(1)),
    `period_end_date_time` DateTime COMMENT 'End of the period, exclusive' CODEC(ZSTD(1)),
    `total_duties` UInt64 COMMENT 'Attestation duties' CODEC(ZSTD(1)),
    `attested_count` UInt64 COMMENT 'Attestations included on the finalized chain' CODEC(ZSTD(1)),
    `missed_count` UInt64 COMMENT 'Attestations missed: duties with no attestation included on the finalized chain' CODEC(ZSTD(1)),
    `head_correct_count` UInt64 COMMENT 'Head votes that were correct' CODEC(ZSTD(1)),
    `target_correct_count` UInt64 COMMENT 'Target votes that were correct' CODEC(ZSTD(1)),
    `source_correct_count` UInt64 COMMENT 'Source votes that were correct, from attestations seen on gossip' CODEC(ZSTD(1)),
    `avg_inclusion_distance` Nullable(Float64) COMMENT 'Mean inclusion distance in slots per included attestation, NULL when there were none' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Canonical blocks proposed' CODEC(ZSTD(1)),
    `mev_block_count` UInt32 COMMENT 'Canonical blocks delivered by an MEV relay, the rest were built locally' CODEC(ZSTD(1)),
    `avg_mev_value_eth` Nullable(Float64) COMMENT 'Mean relay-reported value of the MEV blocks in ETH, NULL when there were none' CODEC(ZSTD(1)),
    `orphaned_block_count` UInt32 COMMENT 'Blocks proposed that did not make the finalized chain' CODEC(ZSTD(1)),
    `missed_slot_count` UInt32 COMMENT 'Slots the entity was scheduled to propose where no block was seen' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) ORDER BY
    (`period`, `entity`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Attestation and block proposal performance per staking entity over rolling periods, refreshed every 30 minutes: one row per period and entity with duties or proposals in it. 24h sums fct_attestation_vote_correctness_by_entity_hourly and fct_block_proposer_by_entity over the last 24 complete hours. The day periods sum fct_attestation_vote_correctness_by_entity_daily and fct_block_proposal_by_entity_daily over the last complete UTC days. The rows of a period sum to the whole network';

CREATE TABLE fct_validator_performance_by_entity_rolling ON CLUSTER '{cluster}' AS fct_validator_performance_by_entity_rolling_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_validator_performance_by_entity_rolling_local,
    cityHash64(`period`, `entity`)
);
