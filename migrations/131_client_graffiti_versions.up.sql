CREATE TABLE dim_client_commit_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client name as used by fct_block_graffiti, e.g. prysm or geth' CODEC(ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `repository` LowCardinality(String) COMMENT 'GitHub repository of the commit' CODEC(ZSTD(1)),
    `commit_sha` String COMMENT 'Full commit hash' CODEC(ZSTD(1)),
    `commit_date` DateTime COMMENT 'Committer date of the commit' CODEC(ZSTD(1)),
    `base_tag` String COMMENT 'Release tag the commit builds on: the tag reachable in the fewest parent steps, as git describe picks it, empty before the first tag' CODEC(ZSTD(1)),
    `base_version` LowCardinality(String) COMMENT 'Version of base_tag, as in dim_client_release' CODEC(ZSTD(1)),
    `commits_since_base` UInt32 COMMENT 'Parent steps along the shortest path from the commit to base_tag, 0 for a tagged commit' CODEC(ZSTD(1)),
    `is_tagged` Bool COMMENT 'Whether a release tag of the client points at this commit' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY
    (`client`, `commit_sha`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Commits on each client default and release branches since 2020, with the release each builds on, so graffiti commits from builds between tags resolve to a version';

CREATE TABLE dim_client_commit ON CLUSTER '{cluster}' AS dim_client_commit_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    dim_client_commit_local,
    cityHash64(`client`, `commit_sha`)
);

CREATE TABLE int_block_graffiti_release_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number of the block' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The root of the canonical beacon block' CODEC(ZSTD(1)),
    `proposer_index` UInt32 COMMENT 'The validator index of the block proposer' CODEC(ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the proposer from dim_node, empty when the validator has none' CODEC(ZSTD(1)),
    `graffiti_format` LowCardinality(String) COMMENT 'Graffiti format from fct_block_graffiti that identified the clients' CODEC(ZSTD(1)),
    `consensus_client` LowCardinality(String) COMMENT 'Consensus client named by the graffiti, e.g. lighthouse, or unknown' CODEC(ZSTD(1)),
    `consensus_client_version` LowCardinality(String) COMMENT 'Consensus client release version from dim_client_release (or the release a branch commit builds on), empty when unresolved' CODEC(ZSTD(1)),
    `consensus_client_commit` LowCardinality(String) COMMENT 'Consensus client commit prefix from the graffiti, lowercase, empty when there is none' CODEC(ZSTD(1)),
    `consensus_client_release_status` LowCardinality(String) COMMENT 'How the consensus client version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `execution_client` LowCardinality(String) COMMENT 'Execution client named by the graffiti, e.g. geth, or unknown' CODEC(ZSTD(1)),
    `execution_client_version` LowCardinality(String) COMMENT 'Execution client release version from dim_client_release (or the release a branch commit builds on), empty when unresolved' CODEC(ZSTD(1)),
    `execution_client_commit` LowCardinality(String) COMMENT 'Execution client commit prefix from the graffiti, lowercase, empty when there is none' CODEC(ZSTD(1)),
    `execution_client_release_status` LowCardinality(String) COMMENT 'How the execution client version was resolved, see the table comment for values' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Canonical blocks with the client releases their graffiti resolves to, resolved like the Lab slot page. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

CREATE TABLE int_block_graffiti_release ON CLUSTER '{cluster}' AS int_block_graffiti_release_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_block_graffiti_release_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE int_block_graffiti_release_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);

CREATE TABLE fct_block_graffiti_client_version_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version the graffiti resolves to, empty when unresolved' CODEC(ZSTD(1)),
    `commit_prefix` LowCardinality(String) COMMENT 'Commit prefix from the graffiti, lowercase, empty when there is none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks with this client, version and release status' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly canonical block counts per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

CREATE TABLE fct_block_graffiti_client_version_hourly ON CLUSTER '{cluster}' AS fct_block_graffiti_client_version_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_version_hourly_local,
    cityHash64(`hour_start_date_time`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
);

CREATE TABLE fct_block_graffiti_client_version_by_entity_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the proposer from dim_node, empty when the validator has none' CODEC(ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version the graffiti resolves to, empty when unresolved' CODEC(ZSTD(1)),
    `commit_prefix` LowCardinality(String) COMMENT 'Commit prefix from the graffiti, lowercase, empty when there is none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks with this client, version and release status' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `entity`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly canonical block counts per staking entity, per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

CREATE TABLE fct_block_graffiti_client_version_by_entity_hourly ON CLUSTER '{cluster}' AS fct_block_graffiti_client_version_by_entity_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_version_by_entity_hourly_local,
    cityHash64(`hour_start_date_time`, `entity`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
);

CREATE TABLE fct_block_graffiti_client_version_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version the graffiti resolves to, empty when unresolved' CODEC(ZSTD(1)),
    `commit_prefix` LowCardinality(String) COMMENT 'Commit prefix from the graffiti, lowercase, empty when there is none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks with this client, version and release status' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily canonical block counts per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

CREATE TABLE fct_block_graffiti_client_version_daily ON CLUSTER '{cluster}' AS fct_block_graffiti_client_version_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_version_daily_local,
    cityHash64(`day_start_date`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
);

CREATE TABLE fct_block_graffiti_client_version_by_entity_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the proposer from dim_node, empty when the validator has none' CODEC(ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version the graffiti resolves to, empty when unresolved' CODEC(ZSTD(1)),
    `commit_prefix` LowCardinality(String) COMMENT 'Commit prefix from the graffiti, lowercase, empty when there is none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks with this client, version and release status' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `entity`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily canonical block counts per staking entity, per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

CREATE TABLE fct_block_graffiti_client_version_by_entity_daily ON CLUSTER '{cluster}' AS fct_block_graffiti_client_version_by_entity_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_version_by_entity_daily_local,
    cityHash64(`day_start_date`, `entity`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
);

CREATE TABLE fct_block_proposal_by_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Proposer client from the block graffiti, or for orphaned blocks and missed slots from the proposer most recent identifying graffiti, else unknown' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Canonical blocks proposed' CODEC(ZSTD(1)),
    `mev_block_count` UInt32 COMMENT 'Canonical blocks delivered by an MEV relay, the rest were built locally' CODEC(ZSTD(1)),
    `avg_mev_value_eth` Nullable(Float64) COMMENT 'Mean relay-reported value of the MEV blocks in ETH, NULL when there were none' CODEC(ZSTD(1)),
    `avg_gas_used` Nullable(Float64) COMMENT 'Mean execution gas used per canonical block' CODEC(ZSTD(1)),
    `avg_blob_count` Nullable(Float64) COMMENT 'Mean blobs per canonical block' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'Blobs in the canonical blocks' CODEC(ZSTD(1)),
    `block_first_seen_p50_ms` Nullable(Float64) COMMENT 'Median over blocks of the median time sentry nodes first saw the block, in ms from slot start' CODEC(ZSTD(1)),
    `block_first_seen_p90_ms` Nullable(Float64) COMMENT '90th percentile over blocks of the median time sentry nodes first saw the block, in ms from slot start' CODEC(ZSTD(1)),
    `orphaned_block_count` UInt32 COMMENT 'Orphaned blocks, attributed through the proposer most recent identifying graffiti' CODEC(ZSTD(1)),
    `missed_slot_count` UInt32 COMMENT 'Missed slots, attributed through the scheduled proposer most recent identifying graffiti' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `layer`, `client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily block proposals per consensus and execution client: blocks, MEV share and value, gas, blobs, first-seen propagation, orphaned blocks and missed slots. Canonical blocks use their own graffiti. Orphaned and missed proposals use the proposer most recent identifying graffiti from the previous 365 days.';

CREATE TABLE fct_block_proposal_by_client_daily ON CLUSTER '{cluster}' AS fct_block_proposal_by_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_proposal_by_client_daily_local,
    cityHash64(`day_start_date`, `layer`, `client`)
);

CREATE TABLE fct_attestation_vote_correctness_by_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client of the validator most recent identifying graffiti in the 365 days before the end of the day, or unknown' CODEC(ZSTD(1)),
    `validator_count` UInt32 COMMENT 'Validators with attestation duties in the day mapped to this client' CODEC(ZSTD(1)),
    `total_duties` UInt64 COMMENT 'Attestation duties' CODEC(ZSTD(1)),
    `attested_count` UInt64 COMMENT 'Attestations included on chain' CODEC(ZSTD(1)),
    `missed_count` UInt64 COMMENT 'Attestations missed' CODEC(ZSTD(1)),
    `head_correct_count` UInt64 COMMENT 'Head votes that were correct' CODEC(ZSTD(1)),
    `target_correct_count` UInt64 COMMENT 'Target votes that were correct' CODEC(ZSTD(1)),
    `source_correct_count` UInt64 COMMENT 'Source votes that were correct' CODEC(ZSTD(1)),
    `avg_inclusion_distance` Nullable(Float64) COMMENT 'Mean inclusion distance per included attestation, NULL when there were none' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `layer`, `client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily attestation vote correctness and inclusion per consensus and execution client. Validators map to the clients of their most recent identifying graffiti (a proposed block naming the client) in the 365 days before the end of the day. Only validators that proposed with identifying graffiti are covered, the rest are the unknown client.';

CREATE TABLE fct_attestation_vote_correctness_by_client_daily ON CLUSTER '{cluster}' AS fct_attestation_vote_correctness_by_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_attestation_vote_correctness_by_client_daily_local,
    cityHash64(`day_start_date`, `layer`, `client`)
);
