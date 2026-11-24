CREATE TABLE fct_block_first_seen_by_node_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
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
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly block arrival per node';

CREATE TABLE fct_block_first_seen_by_node_hourly ON CLUSTER '{cluster}' AS fct_block_first_seen_by_node_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_first_seen_by_node_hourly_local,
    cityHash64(`hour_start_date_time`, `node_id`)
);

ALTER TABLE fct_block_first_seen_by_node_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `hour_start_date_time`)
);

CREATE TABLE fct_block_first_seen_by_node_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
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
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily block arrival per node';

CREATE TABLE fct_block_first_seen_by_node_daily ON CLUSTER '{cluster}' AS fct_block_first_seen_by_node_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_first_seen_by_node_daily_local,
    cityHash64(`day_start_date`, `node_id`)
);

ALTER TABLE fct_block_first_seen_by_node_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `day_start_date`)
);

CREATE TABLE fct_head_first_seen_by_node_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
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
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly head event arrival per node';

CREATE TABLE fct_head_first_seen_by_node_hourly ON CLUSTER '{cluster}' AS fct_head_first_seen_by_node_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_head_first_seen_by_node_hourly_local,
    cityHash64(`hour_start_date_time`, `node_id`)
);

ALTER TABLE fct_head_first_seen_by_node_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `hour_start_date_time`)
);

CREATE TABLE fct_head_first_seen_by_node_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
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
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily head event arrival per node';

CREATE TABLE fct_head_first_seen_by_node_daily ON CLUSTER '{cluster}' AS fct_head_first_seen_by_node_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_head_first_seen_by_node_daily_local,
    cityHash64(`day_start_date`, `node_id`)
);

ALTER TABLE fct_head_first_seen_by_node_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `day_start_date`)
);

CREATE TABLE fct_attestation_observation_by_node_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
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
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly attestation arrival per node';

CREATE TABLE fct_attestation_observation_by_node_hourly ON CLUSTER '{cluster}' AS fct_attestation_observation_by_node_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_attestation_observation_by_node_hourly_local,
    cityHash64(`hour_start_date_time`, `node_id`)
);

ALTER TABLE fct_attestation_observation_by_node_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `hour_start_date_time`)
);

CREATE TABLE fct_attestation_observation_by_node_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
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
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily attestation arrival per node';

CREATE TABLE fct_attestation_observation_by_node_daily ON CLUSTER '{cluster}' AS fct_attestation_observation_by_node_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_attestation_observation_by_node_daily_local,
    cityHash64(`day_start_date`, `node_id`)
);

ALTER TABLE fct_attestation_observation_by_node_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `day_start_date`)
);

CREATE TABLE fct_block_data_column_sidecar_first_seen_by_node_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
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
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly data column arrival per node';

CREATE TABLE fct_block_data_column_sidecar_first_seen_by_node_hourly ON CLUSTER '{cluster}' AS fct_block_data_column_sidecar_first_seen_by_node_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_data_column_sidecar_first_seen_by_node_hourly_local,
    cityHash64(`hour_start_date_time`, `node_id`)
);

ALTER TABLE fct_block_data_column_sidecar_first_seen_by_node_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `hour_start_date_time`)
);

CREATE TABLE fct_block_data_column_sidecar_first_seen_by_node_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
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
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily data column arrival per node';

CREATE TABLE fct_block_data_column_sidecar_first_seen_by_node_daily ON CLUSTER '{cluster}' AS fct_block_data_column_sidecar_first_seen_by_node_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_data_column_sidecar_first_seen_by_node_daily_local,
    cityHash64(`day_start_date`, `node_id`)
);

ALTER TABLE fct_block_data_column_sidecar_first_seen_by_node_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `day_start_date`)
);

CREATE TABLE fct_engine_new_payload_by_node_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Latest client name of the node in the period' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Latest collector implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Latest collector version of the node in the period' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Latest execution client implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_execution_version` LowCardinality(String) COMMENT 'Latest execution client version of the node in the period' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots with an engine_newPayload call' CODEC(ZSTD(1)),
    `call_count` UInt32 COMMENT 'engine_newPayload calls' CODEC(ZSTD(1)),
    `valid_count` UInt32 COMMENT 'Calls that returned VALID' CODEC(ZSTD(1)),
    `invalid_count` UInt32 COMMENT 'Calls that returned INVALID' CODEC(ZSTD(1)),
    `syncing_count` UInt32 COMMENT 'Calls that returned SYNCING' CODEC(ZSTD(1)),
    `accepted_count` UInt32 COMMENT 'Calls that returned ACCEPTED' CODEC(ZSTD(1)),
    `p50_duration_ms` UInt32 COMMENT 'Median duration of VALID calls, in milliseconds' CODEC(ZSTD(1)),
    `p90_duration_ms` UInt32 COMMENT '90th percentile duration of VALID calls, in milliseconds' CODEC(ZSTD(1)),
    `p95_duration_ms` UInt32 COMMENT '95th percentile duration of VALID calls, in milliseconds' CODEC(ZSTD(1)),
    `max_duration_ms` UInt32 COMMENT 'Longest VALID call, in milliseconds' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly engine_newPayload duration per node';

CREATE TABLE fct_engine_new_payload_by_node_hourly ON CLUSTER '{cluster}' AS fct_engine_new_payload_by_node_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_by_node_hourly_local,
    cityHash64(`hour_start_date_time`, `node_id`)
);

ALTER TABLE fct_engine_new_payload_by_node_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `hour_start_date_time`)
);

CREATE TABLE fct_engine_new_payload_by_node_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Latest client name of the node in the period' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Latest collector implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Latest collector version of the node in the period' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Latest execution client implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_execution_version` LowCardinality(String) COMMENT 'Latest execution client version of the node in the period' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots with an engine_newPayload call' CODEC(ZSTD(1)),
    `call_count` UInt32 COMMENT 'engine_newPayload calls' CODEC(ZSTD(1)),
    `valid_count` UInt32 COMMENT 'Calls that returned VALID' CODEC(ZSTD(1)),
    `invalid_count` UInt32 COMMENT 'Calls that returned INVALID' CODEC(ZSTD(1)),
    `syncing_count` UInt32 COMMENT 'Calls that returned SYNCING' CODEC(ZSTD(1)),
    `accepted_count` UInt32 COMMENT 'Calls that returned ACCEPTED' CODEC(ZSTD(1)),
    `p50_duration_ms` UInt32 COMMENT 'Median duration of VALID calls, in milliseconds' CODEC(ZSTD(1)),
    `p90_duration_ms` UInt32 COMMENT '90th percentile duration of VALID calls, in milliseconds' CODEC(ZSTD(1)),
    `p95_duration_ms` UInt32 COMMENT '95th percentile duration of VALID calls, in milliseconds' CODEC(ZSTD(1)),
    `max_duration_ms` UInt32 COMMENT 'Longest VALID call, in milliseconds' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily engine_newPayload duration per node';

CREATE TABLE fct_engine_new_payload_by_node_daily ON CLUSTER '{cluster}' AS fct_engine_new_payload_by_node_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_by_node_daily_local,
    cityHash64(`day_start_date`, `node_id`)
);

ALTER TABLE fct_engine_new_payload_by_node_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `day_start_date`)
);

CREATE TABLE fct_engine_get_blobs_by_node_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Latest client name of the node in the period' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Latest collector implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Latest collector version of the node in the period' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Latest execution client implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_execution_version` LowCardinality(String) COMMENT 'Latest execution client version of the node in the period' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots with an engine_getBlobs call' CODEC(ZSTD(1)),
    `call_count` UInt32 COMMENT 'engine_getBlobs calls' CODEC(ZSTD(1)),
    `requested_count` UInt64 COMMENT 'Blobs requested across every call' CODEC(ZSTD(1)),
    `returned_count` UInt64 COMMENT 'Blobs returned across every call' CODEC(ZSTD(1)),
    `p50_duration_ms` UInt32 COMMENT 'Median call duration, in milliseconds' CODEC(ZSTD(1)),
    `p90_duration_ms` UInt32 COMMENT '90th percentile call duration, in milliseconds' CODEC(ZSTD(1)),
    `p95_duration_ms` UInt32 COMMENT '95th percentile call duration, in milliseconds' CODEC(ZSTD(1)),
    `max_duration_ms` UInt32 COMMENT 'Longest call, in milliseconds' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly engine_getBlobs duration per node';

CREATE TABLE fct_engine_get_blobs_by_node_hourly ON CLUSTER '{cluster}' AS fct_engine_get_blobs_by_node_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_get_blobs_by_node_hourly_local,
    cityHash64(`hour_start_date_time`, `node_id`)
);

ALTER TABLE fct_engine_get_blobs_by_node_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `hour_start_date_time`)
);

CREATE TABLE fct_engine_get_blobs_by_node_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Latest client name of the node in the period' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Latest collector implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Latest collector version of the node in the period' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Latest execution client implementation of the node in the period' CODEC(ZSTD(1)),
    `meta_execution_version` LowCardinality(String) COMMENT 'Latest execution client version of the node in the period' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Slots with an engine_getBlobs call' CODEC(ZSTD(1)),
    `call_count` UInt32 COMMENT 'engine_getBlobs calls' CODEC(ZSTD(1)),
    `requested_count` UInt64 COMMENT 'Blobs requested across every call' CODEC(ZSTD(1)),
    `returned_count` UInt64 COMMENT 'Blobs returned across every call' CODEC(ZSTD(1)),
    `p50_duration_ms` UInt32 COMMENT 'Median call duration, in milliseconds' CODEC(ZSTD(1)),
    `p90_duration_ms` UInt32 COMMENT '90th percentile call duration, in milliseconds' CODEC(ZSTD(1)),
    `p95_duration_ms` UInt32 COMMENT '95th percentile call duration, in milliseconds' CODEC(ZSTD(1)),
    `max_duration_ms` UInt32 COMMENT 'Longest call, in milliseconds' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `node_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily engine_getBlobs duration per node';

CREATE TABLE fct_engine_get_blobs_by_node_daily ON CLUSTER '{cluster}' AS fct_engine_get_blobs_by_node_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_get_blobs_by_node_daily_local,
    cityHash64(`day_start_date`, `node_id`)
);

ALTER TABLE fct_engine_get_blobs_by_node_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_node_id
(
    SELECT *
    ORDER BY (`node_id`, `day_start_date`)
);
