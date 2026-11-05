CREATE TABLE fct_block_first_seen_by_network_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_count` UInt32 COMMENT 'Xatu nodes with a row in fct_block_first_seen_by_node_hourly for the hour' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots in the hour that at least one Xatu node saw a block in' CODEC(ZSTD(1)),
    `p10_median_seen_slot_start_diff` Float64 COMMENT '10th percentile across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_hourly), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1)),
    `p50_median_seen_slot_start_diff` Float64 COMMENT 'Median across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_hourly), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1)),
    `p90_median_seen_slot_start_diff` Float64 COMMENT '90th percentile across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_hourly), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly block arrival across every Xatu node: the spread of the per-node medians in fct_block_first_seen_by_node_hourly, one row per hour';

CREATE TABLE fct_block_first_seen_by_network_hourly ON CLUSTER '{cluster}' AS fct_block_first_seen_by_network_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_first_seen_by_network_hourly_local,
    cityHash64(`hour_start_date_time`)
);

CREATE TABLE fct_block_first_seen_by_network_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_count` UInt32 COMMENT 'Xatu nodes with a row in fct_block_first_seen_by_node_daily for the day' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots in the day that at least one Xatu node saw a block in' CODEC(ZSTD(1)),
    `p10_median_seen_slot_start_diff` Float64 COMMENT '10th percentile across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_daily), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1)),
    `p50_median_seen_slot_start_diff` Float64 COMMENT 'Median across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_daily), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1)),
    `p90_median_seen_slot_start_diff` Float64 COMMENT '90th percentile across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_daily), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY
    (`day_start_date`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily block arrival across every Xatu node: the spread of the per-node medians in fct_block_first_seen_by_node_daily, one row per day';

CREATE TABLE fct_block_first_seen_by_network_daily ON CLUSTER '{cluster}' AS fct_block_first_seen_by_network_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_first_seen_by_network_daily_local,
    cityHash64(`day_start_date`)
);

CREATE TABLE fct_block_first_seen_by_network_weekly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC), by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_count` UInt32 COMMENT 'Xatu nodes with a row in fct_block_first_seen_by_node_weekly for the week' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots in the week that at least one Xatu node saw a block in' CODEC(ZSTD(1)),
    `p10_median_seen_slot_start_diff` Float64 COMMENT '10th percentile across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_weekly), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1)),
    `p50_median_seen_slot_start_diff` Float64 COMMENT 'Median across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_weekly), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1)),
    `p90_median_seen_slot_start_diff` Float64 COMMENT '90th percentile across nodes of each node median block arrival (p50_seen_slot_start_diff in fct_block_first_seen_by_node_weekly), in milliseconds from slot start. Every node weighs the same, linear interpolation between ranks' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY
    (`week_start_date`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Weekly block arrival across every Xatu node: the spread of the per-node medians in fct_block_first_seen_by_node_weekly, one row per week (Monday to Sunday, UTC)';

CREATE TABLE fct_block_first_seen_by_network_weekly ON CLUSTER '{cluster}' AS fct_block_first_seen_by_network_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_first_seen_by_network_weekly_local,
    cityHash64(`week_start_date`)
);

CREATE TABLE fct_block_first_seen_by_node_weekly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC), by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Latest client name of the node in the period' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Latest collector implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Latest collector version of the node in the period' CODEC(ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Latest consensus client implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_consensus_version` LowCardinality(String) COMMENT 'Latest consensus client version of the node in the period' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots the node saw a block in' CODEC(ZSTD(1)),
    `min_seen_slot_start_diff` UInt32 COMMENT 'Earliest block arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p10_seen_slot_start_diff` UInt32 COMMENT '10th percentile block arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_seen_slot_start_diff` UInt32 COMMENT 'Median block arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_seen_slot_start_diff` UInt32 COMMENT '90th percentile block arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p95_seen_slot_start_diff` UInt32 COMMENT '95th percentile block arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `max_seen_slot_start_diff` UInt32 COMMENT 'Latest block arrival, in milliseconds from slot start' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY
    (`week_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Weekly block arrival per node, weeks run Monday to Sunday (UTC)';

CREATE TABLE fct_block_first_seen_by_node_weekly ON CLUSTER '{cluster}' AS fct_block_first_seen_by_node_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_first_seen_by_node_weekly_local,
    cityHash64(`week_start_date`, `node_id`)
);

ALTER TABLE fct_block_first_seen_by_node_weekly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `week_start_date`)
);

CREATE TABLE fct_head_first_seen_by_node_weekly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC), by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Latest client name of the node in the period' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Latest collector implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Latest collector version of the node in the period' CODEC(ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Latest consensus client implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_consensus_version` LowCardinality(String) COMMENT 'Latest consensus client version of the node in the period' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots the node saw a head event in' CODEC(ZSTD(1)),
    `min_seen_slot_start_diff` UInt32 COMMENT 'Earliest head event arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p10_seen_slot_start_diff` UInt32 COMMENT '10th percentile head event arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_seen_slot_start_diff` UInt32 COMMENT 'Median head event arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_seen_slot_start_diff` UInt32 COMMENT '90th percentile head event arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p95_seen_slot_start_diff` UInt32 COMMENT '95th percentile head event arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `max_seen_slot_start_diff` UInt32 COMMENT 'Latest head event arrival, in milliseconds from slot start' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY
    (`week_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Weekly head event arrival per node, weeks run Monday to Sunday (UTC)';

CREATE TABLE fct_head_first_seen_by_node_weekly ON CLUSTER '{cluster}' AS fct_head_first_seen_by_node_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_head_first_seen_by_node_weekly_local,
    cityHash64(`week_start_date`, `node_id`)
);

ALTER TABLE fct_head_first_seen_by_node_weekly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `week_start_date`)
);

CREATE TABLE fct_attestation_observation_by_node_weekly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC), by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Latest client name of the node in the period' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Latest collector implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Latest collector version of the node in the period' CODEC(ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Latest consensus client implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_consensus_version` LowCardinality(String) COMMENT 'Latest consensus client version of the node in the period' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots the node saw attestations in' CODEC(ZSTD(1)),
    `attestation_count` UInt64 COMMENT 'Attestations the node saw' CODEC(ZSTD(1)),
    `min_seen_slot_start_diff` UInt32 COMMENT 'Earliest attestation arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p10_median_seen_slot_start_diff` UInt32 COMMENT '10th percentile of the per-slot median attestation arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_median_seen_slot_start_diff` UInt32 COMMENT 'Median of the per-slot median attestation arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_median_seen_slot_start_diff` UInt32 COMMENT '90th percentile of the per-slot median attestation arrival, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p95_median_seen_slot_start_diff` UInt32 COMMENT '95th percentile of the per-slot median attestation arrival, in milliseconds from slot start' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY
    (`week_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Weekly attestation arrival per node, weeks run Monday to Sunday (UTC)';

CREATE TABLE fct_attestation_observation_by_node_weekly ON CLUSTER '{cluster}' AS fct_attestation_observation_by_node_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_attestation_observation_by_node_weekly_local,
    cityHash64(`week_start_date`, `node_id`)
);

ALTER TABLE fct_attestation_observation_by_node_weekly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `week_start_date`)
);

CREATE TABLE fct_block_data_column_sidecar_first_seen_by_node_weekly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC), by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Latest client name of the node in the period' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Latest collector implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Latest collector version of the node in the period' CODEC(ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Latest consensus client implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_consensus_version` LowCardinality(String) COMMENT 'Latest consensus client version of the node in the period' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots the node saw data columns in' CODEC(ZSTD(1)),
    `column_count` UInt16 COMMENT 'Distinct column indexes the node saw, its custody set' CODEC(ZSTD(1)),
    `avg_columns_per_slot` Float64 COMMENT 'Average columns the node saw per slot' CODEC(ZSTD(1)),
    `p50_first_seen_slot_start_diff` UInt32 COMMENT 'Median arrival of the first column in a slot, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p10_last_seen_slot_start_diff` UInt32 COMMENT '10th percentile arrival of the last column in a slot, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_last_seen_slot_start_diff` UInt32 COMMENT 'Median arrival of the last column in a slot, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_last_seen_slot_start_diff` UInt32 COMMENT '90th percentile arrival of the last column in a slot, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p95_last_seen_slot_start_diff` UInt32 COMMENT '95th percentile arrival of the last column in a slot, in milliseconds from slot start' CODEC(ZSTD(1)),
    `max_last_seen_slot_start_diff` UInt32 COMMENT 'Latest arrival of the last column in a slot, in milliseconds from slot start' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY
    (`week_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Weekly data column arrival per node, weeks run Monday to Sunday (UTC)';

CREATE TABLE fct_block_data_column_sidecar_first_seen_by_node_weekly ON CLUSTER '{cluster}' AS fct_block_data_column_sidecar_first_seen_by_node_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_data_column_sidecar_first_seen_by_node_weekly_local,
    cityHash64(`week_start_date`, `node_id`)
);

ALTER TABLE fct_block_data_column_sidecar_first_seen_by_node_weekly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `week_start_date`)
);

CREATE TABLE fct_block_graffiti_client_version_by_entity_weekly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the proposer from dim_node, empty when the validator has none' CODEC(ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version the graffiti resolves to, else the version it states, empty when unresolved' CODEC(ZSTD(1)),
    `commit_prefix` LowCardinality(String) COMMENT 'Commit prefix from the graffiti, lowercase, empty when there is none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks with this client, version and release status' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY
    (`week_start_date`, `entity`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Weekly (Monday to Sunday, UTC) canonical block counts per staking entity, per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the commit is a release tag commit agreeing with any version the graffiti states), commit_untagged (a default or release branch commit between tags, version is the release it builds on, else the stated version, empty for a development build with no recent tag), commit_mismatch (the commit resolves to another release than the stated version, which some clients leave stale, version is the commit release), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Rows whose commit does not resolve keep the version the graffiti states. Rows with neither have an empty version';

CREATE TABLE fct_block_graffiti_client_version_by_entity_weekly ON CLUSTER '{cluster}' AS fct_block_graffiti_client_version_by_entity_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_version_by_entity_weekly_local,
    cityHash64(`week_start_date`, `entity`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
);

CREATE TABLE fct_block_graffiti_client_by_entity_weekly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `entity` LowCardinality(String) COMMENT 'Staking entity of the proposer from dim_node, empty when the validator has none' CODEC(ZSTD(1)),
    `consensus_client` LowCardinality(String) COMMENT 'Consensus client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `execution_client` LowCardinality(String) COMMENT 'Execution client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks in the week from this entity with this consensus and execution client pair' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY
    (`week_start_date`, `entity`, `consensus_client`, `execution_client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Weekly (Monday to Sunday, UTC) canonical block counts per proposer entity and consensus and execution client pair named by block graffiti';

CREATE TABLE fct_block_graffiti_client_by_entity_weekly ON CLUSTER '{cluster}' AS fct_block_graffiti_client_by_entity_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_by_entity_weekly_local,
    cityHash64(`week_start_date`, `entity`, `consensus_client`, `execution_client`)
);

CREATE TABLE fct_block_graffiti_client_version_by_entity_group_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `entity_group` LowCardinality(String) COMMENT 'Group of staking entities run under one protocol: lido is every entity whose name contains lido (lido itself, the curated *_lido operators, the csm_operator*_lido Community Staking operators and the lidox* Simple DVT clusters)' CODEC(ZSTD(1)),
    `layer` LowCardinality(String) COMMENT 'Client layer: consensus or execution' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version the graffiti resolves to, else the version it states, empty when unresolved' CODEC(ZSTD(1)),
    `commit_prefix` LowCardinality(String) COMMENT 'Commit prefix from the graffiti, lowercase, empty when there is none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `entity_count` UInt32 COMMENT 'Entities of the group with a block of this client, version and release status in the day' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks with this client, version and release status' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `entity_group`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily canonical block counts per entity group, the sum of the member entities in fct_block_graffiti_client_version_by_entity_daily, per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the commit is a release tag commit agreeing with any version the graffiti states), commit_untagged (a default or release branch commit between tags, version is the release it builds on, else the stated version, empty for a development build with no recent tag), commit_mismatch (the commit resolves to another release than the stated version, which some clients leave stale, version is the commit release), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Rows whose commit does not resolve keep the version the graffiti states. Rows with neither have an empty version. Only entities in a group have rows';

CREATE TABLE fct_block_graffiti_client_version_by_entity_group_daily ON CLUSTER '{cluster}' AS fct_block_graffiti_client_version_by_entity_group_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_version_by_entity_group_daily_local,
    cityHash64(`day_start_date`, `entity_group`, `layer`, `client`, `version`, `commit_prefix`, `release_status`)
);

CREATE TABLE fct_block_graffiti_client_by_entity_group_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `entity_group` LowCardinality(String) COMMENT 'Group of staking entities run under one protocol: lido is every entity whose name contains lido (lido itself, the curated *_lido operators, the csm_operator*_lido Community Staking operators and the lidox* Simple DVT clusters)' CODEC(ZSTD(1)),
    `consensus_client` LowCardinality(String) COMMENT 'Consensus client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `execution_client` LowCardinality(String) COMMENT 'Execution client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `entity_count` UInt32 COMMENT 'Entities of the group with a block from this consensus and execution client pair in the day' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks in the day from the group with this consensus and execution client pair' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `entity_group`, `consensus_client`, `execution_client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily canonical block counts per entity group and consensus and execution client pair named by block graffiti: the sum of the member entities in fct_block_graffiti_client_by_entity_daily. Only entities in a group have rows';

CREATE TABLE fct_block_graffiti_client_by_entity_group_daily ON CLUSTER '{cluster}' AS fct_block_graffiti_client_by_entity_group_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_by_entity_group_daily_local,
    cityHash64(`day_start_date`, `entity_group`, `consensus_client`, `execution_client`)
);

CREATE TABLE fct_opcode_gas_by_opcode_weekly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC), by block timestamp' CODEC(DoubleDelta, ZSTD(1)),
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `block_count` UInt32 COMMENT 'Number of blocks containing this opcode in this week' CODEC(ZSTD(1)),
    `total_count` UInt64 COMMENT 'Total execution count of this opcode in this week' CODEC(ZSTD(1)),
    `total_gas` UInt64 COMMENT 'Total gas consumed by this opcode in this week' CODEC(ZSTD(1)),
    `total_error_count` UInt64 COMMENT 'Total error count for this opcode in this week' CODEC(ZSTD(1)),
    `avg_count_per_block` Float32 COMMENT 'Average executions per block' CODEC(ZSTD(1)),
    `avg_gas_per_block` Float32 COMMENT 'Average gas per block' CODEC(ZSTD(1)),
    `avg_gas_per_execution` Float32 COMMENT 'Average gas per execution' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY
    (`week_start_date`, `opcode`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Weekly (Monday to Sunday, UTC) per-opcode gas consumption for Top Opcodes by Gas charts, summed from fct_opcode_gas_by_opcode_daily';

CREATE TABLE fct_opcode_gas_by_opcode_weekly ON CLUSTER '{cluster}' AS fct_opcode_gas_by_opcode_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_opcode_gas_by_opcode_weekly_local,
    cityHash64(`week_start_date`)
);

ALTER TABLE fct_opcode_gas_by_opcode_weekly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode
(
    SELECT *
    ORDER BY (`opcode`, `week_start_date`)
);

CREATE TABLE fct_prepared_block_by_node_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the node that built the blocks' CODEC(ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Consensus client implementation of the node' CODEC(ZSTD(1)),
    `meta_consensus_version` LowCardinality(String) COMMENT 'Consensus client version of the node' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Locally built blocks (rows in fct_prepared_block) from the node with this consensus client version in the day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY
    (`day_start_date`, `meta_client_name`, `meta_consensus_implementation`, `meta_consensus_version`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily locally built block counts per Xatu node and consensus client version, from fct_prepared_block';

CREATE TABLE fct_prepared_block_by_node_daily ON CLUSTER '{cluster}' AS fct_prepared_block_by_node_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_prepared_block_by_node_daily_local,
    cityHash64(`day_start_date`, `meta_client_name`)
);

CREATE TABLE fct_opcode_cost_by_el_client_weekly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week_start_date` Date COMMENT 'Start of the week period (Monday, UTC), by block timestamp' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification the timings come from (e.g., eip7870-block-builder)',
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation (e.g., Geth, Nethermind, Besu, Reth)',
    `opcode_group` LowCardinality(String) COMMENT 'Opcode, or opcode group (LOG, ARITHMETIC, CONTROL, ENVIRONMENT, MEMORY, STACK)',
    `meta_execution_versions` Array(String) COMMENT 'Client versions that ran during the week, most blocks first' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Block observations in the fit' CODEC(ZSTD(1)),
    `hour_count` UInt32 COMMENT 'Hours with data in the fit' CODEC(ZSTD(1)),
    `gas_share` Float64 COMMENT 'Share of all opcode and non-opcode gas spent on this opcode group' CODEC(ZSTD(1)),
    `gas_per_op` Float64 COMMENT 'Average gas charged per execution of this opcode group' CODEC(ZSTD(1)),
    `ns_per_gas` Float64 COMMENT 'Fitted execution time per unit of gas for this opcode group, in nanoseconds' CODEC(ZSTD(1)),
    `ns_per_op` Float64 COMMENT 'Fitted execution time per execution of this opcode group, in nanoseconds' CODEC(ZSTD(1)),
    `ns_per_op_lower` Float64 COMMENT '5th percentile of ns_per_op over an hourly bootstrap' CODEC(ZSTD(1)),
    `ns_per_op_upper` Float64 COMMENT '95th percentile of ns_per_op over an hourly bootstrap' CODEC(ZSTD(1)),
    `price_ratio` Float64 COMMENT 'ns_per_gas divided by the client average ns per gas, where 1 means gas prices this opcode group in line with the rest of execution' CODEC(ZSTD(1)),
    `price_ratio_lower` Float64 COMMENT '5th percentile of price_ratio over an hourly bootstrap' CODEC(ZSTD(1)),
    `price_ratio_upper` Float64 COMMENT '95th percentile of price_ratio over an hourly bootstrap' CODEC(ZSTD(1)),
    `client_ns_per_gas` Float64 COMMENT 'Client average execution time per unit of gas, in nanoseconds' CODEC(ZSTD(1)),
    `overhead_ms` Float64 COMMENT 'Fitted fixed cost per block, in milliseconds' CODEC(ZSTD(1)),
    `per_tx_us` Float64 COMMENT 'Fitted cost per transaction, in microseconds' CODEC(ZSTD(1)),
    `r_squared` Float64 COMMENT 'Share of block duration variance the fit explains' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY (`week_start_date`, `node_class`, `meta_execution_implementation`, `opcode_group`)
COMMENT 'Weekly (Monday to Sunday, UTC) execution cost per opcode group by execution client, fitted from engine_newPayload durations over the whole week';

CREATE TABLE fct_opcode_cost_by_el_client_weekly ON CLUSTER '{cluster}' AS fct_opcode_cost_by_el_client_weekly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_opcode_cost_by_el_client_weekly_local,
    cityHash64(`week_start_date`, `node_class`, `meta_execution_implementation`, `opcode_group`)
);

ALTER TABLE fct_data_column_availability_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by UTC day and column index, rolled up from fct_data_column_availability_by_epoch through its hours like fct_data_column_availability_hourly: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by UTC day and column index, rolled up from fct_data_column_availability_by_epoch through its hours like fct_data_column_availability_hourly: almost all observations are gossipsub sightings, which always count as a success';
