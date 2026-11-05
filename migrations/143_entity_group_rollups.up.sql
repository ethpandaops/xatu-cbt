CREATE TABLE fct_validator_stake_by_entity_group_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `entity_group` LowCardinality(String) COMMENT 'Group of staking entities run under one protocol: lido is every entity whose name contains lido (lido itself, the curated *_lido operators, the csm_operator*_lido Community Staking operators and the lidox* Simple DVT clusters)' CODEC(ZSTD(1)),
    `credential_type` LowCardinality(String) COMMENT 'Withdrawal credential prefix at the snapshot: 0x00, 0x01 or 0x02' CODEC(ZSTD(1)),
    `entity_count` UInt32 COMMENT 'Entities of the group with active validators of this credential type on the day' CODEC(ZSTD(1)),
    `active_validator_count` UInt32 COMMENT 'Active validators (active_ongoing, active_exiting, active_slashed)' CODEC(ZSTD(1)),
    `active_effective_balance` UInt64 COMMENT 'Sum of active validators'' effective balance in Gwei (active stake)' CODEC(ZSTD(1)),
    `active_balance` UInt64 COMMENT 'Sum of active validators'' balance in Gwei' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY
    (`day_start_date`, `entity_group`, `credential_type`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Active validators and stake per entity group and withdrawal credential type, one snapshot per day: the sum of the member entities in fct_validator_stake_by_entity_daily. Only entities in a group have rows';

CREATE TABLE fct_validator_stake_by_entity_group_daily ON CLUSTER '{cluster}' AS fct_validator_stake_by_entity_group_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_validator_stake_by_entity_group_daily_local,
    cityHash64(`day_start_date`, `entity_group`)
);

CREATE TABLE fct_attestation_vote_correctness_by_entity_group_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `entity_group` LowCardinality(String) COMMENT 'Group of staking entities run under one protocol: lido is every entity whose name contains lido (lido itself, the curated *_lido operators, the csm_operator*_lido Community Staking operators and the lidox* Simple DVT clusters)' CODEC(ZSTD(1)),
    `entity_count` UInt32 COMMENT 'Entities of the group with attestation duties in the day' CODEC(ZSTD(1)),
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
    (`day_start_date`, `entity_group`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily attestation liveness, vote correctness and inclusion per entity group on the finalized chain: the sum of the member entities in fct_attestation_vote_correctness_by_entity_daily. Only entities in a group have rows';

CREATE TABLE fct_attestation_vote_correctness_by_entity_group_daily ON CLUSTER '{cluster}' AS fct_attestation_vote_correctness_by_entity_group_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_attestation_vote_correctness_by_entity_group_daily_local,
    cityHash64(`day_start_date`, `entity_group`)
);

CREATE TABLE fct_block_proposal_by_entity_group_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `entity_group` LowCardinality(String) COMMENT 'Group of staking entities run under one protocol: lido is every entity whose name contains lido (lido itself, the curated *_lido operators, the csm_operator*_lido Community Staking operators and the lidox* Simple DVT clusters)' CODEC(ZSTD(1)),
    `entity_count` UInt32 COMMENT 'Entities of the group with a proposal in the day' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Canonical blocks proposed' CODEC(ZSTD(1)),
    `mev_block_count` UInt32 COMMENT 'Canonical blocks delivered by an MEV relay, the rest were built locally' CODEC(ZSTD(1)),
    `avg_mev_value_eth` Nullable(Float64) COMMENT 'Mean relay-reported value of the MEV blocks in ETH, NULL when there were none' CODEC(ZSTD(1)),
    `orphaned_block_count` UInt32 COMMENT 'Blocks proposed that did not make the finalized chain' CODEC(ZSTD(1)),
    `missed_slot_count` UInt32 COMMENT 'Slots the group was scheduled to propose where no block was seen' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `entity_group`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily block proposals per entity group on the finalized chain: the sum of the member entities in fct_block_proposal_by_entity_daily. Proposals due are block_count + orphaned_block_count + missed_slot_count. Only entities in a group have rows';

CREATE TABLE fct_block_proposal_by_entity_group_daily ON CLUSTER '{cluster}' AS fct_block_proposal_by_entity_group_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_proposal_by_entity_group_daily_local,
    cityHash64(`day_start_date`, `entity_group`)
);
