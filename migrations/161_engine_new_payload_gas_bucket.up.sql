CREATE TABLE fct_engine_new_payload_gas_bucket_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour' CODEC(DoubleDelta, ZSTD(1)),
    `node_scope` LowCardinality(String) COMMENT 'Which Xatu nodes the timings come from: reference for reference nodes (node_class eip7870-block-builder), all for every Xatu node',
    `client` LowCardinality(String) COMMENT 'Execution client, lowercased, with go-ethereum as geth',
    `gas_bucket_mgas` UInt16 COMMENT 'Block size bucket, the lower bound in Mgas of a 5 Mgas wide bucket' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks' CODEC(ZSTD(1)),
    `gas_mgas_sum` Float64 COMMENT 'Sum of block gas used in Mgas' CODEC(ZSTD(1)),
    `gas_mgas_sq_sum` Float64 COMMENT 'Sum of squared block gas used in Mgas' CODEC(ZSTD(1)),
    `avg_ms_sum` Float64 COMMENT 'Sum over blocks of the mean engine_newPayload duration across Xatu nodes, in milliseconds' CODEC(ZSTD(1)),
    `p50_ms_sum` Float64 COMMENT 'Sum over blocks of the median engine_newPayload duration across Xatu nodes, in milliseconds' CODEC(ZSTD(1)),
    `p50_ms_sq_sum` Float64 COMMENT 'Sum over blocks of the squared median duration' CODEC(ZSTD(1)),
    `gas_p50_product_sum` Float64 COMMENT 'Sum over blocks of Mgas times the median duration' CODEC(ZSTD(1)),
    `p95_ms_sum` Float64 COMMENT 'Sum over blocks of the 95th percentile engine_newPayload duration across Xatu nodes, in milliseconds' CODEC(ZSTD(1)),
    `p95_ms_sq_sum` Float64 COMMENT 'Sum over blocks of the squared 95th percentile duration' CODEC(ZSTD(1)),
    `gas_p95_product_sum` Float64 COMMENT 'Sum over blocks of Mgas times the 95th percentile duration' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`, `node_scope`, `client`, `gas_bucket_mgas`)
COMMENT 'engine_newPayload timings of VALID payloads per hour, node scope, execution client and 5 Mgas block size bucket, as counts and sums from which a least-squares cost model and throughput can be computed exactly over any range';

CREATE TABLE fct_engine_new_payload_gas_bucket_hourly ON CLUSTER '{cluster}' AS fct_engine_new_payload_gas_bucket_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_gas_bucket_hourly_local,
    cityHash64(`hour_start_date_time`, `client`)
);

CREATE TABLE fct_engine_new_payload_throughput_histogram_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour' CODEC(DoubleDelta, ZSTD(1)),
    `node_scope` LowCardinality(String) COMMENT 'Which Xatu nodes the timings come from: reference for reference nodes (node_class eip7870-block-builder), all for every Xatu node',
    `client` LowCardinality(String) COMMENT 'Execution client, lowercased, with go-ethereum as geth',
    `bucket_index` Int16 COMMENT 'Throughput bucket k, covering 1.04^k to 1.04^(k+1) Mgas/s' CODEC(ZSTD(1)),
    `bucket_lower_mgas_per_second` Float32 COMMENT 'Lower bound of the bucket in Mgas/s, inclusive' CODEC(ZSTD(1)),
    `bucket_upper_mgas_per_second` Float32 COMMENT 'Upper bound of the bucket in Mgas/s, exclusive' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks of at least 5 Mgas whose throughput falls in the bucket' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`, `node_scope`, `client`, `bucket_index`)
COMMENT 'Per-block engine_newPayload throughput histogram (Mgas/s, buckets about 4% wide) of VALID payloads of at least 5 Mgas per hour, node scope and execution client';

CREATE TABLE fct_engine_new_payload_throughput_histogram_hourly ON CLUSTER '{cluster}' AS fct_engine_new_payload_throughput_histogram_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_throughput_histogram_hourly_local,
    cityHash64(`hour_start_date_time`, `client`)
);

CREATE TABLE fct_engine_new_payload_resource_share_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour' CODEC(DoubleDelta, ZSTD(1)),
    `node_scope` LowCardinality(String) COMMENT 'Which Xatu nodes the timings come from: reference for reference nodes (node_class eip7870-block-builder), all for every Xatu node',
    `client` LowCardinality(String) COMMENT 'Execution client, lowercased, with go-ethereum as geth',
    `resource` LowCardinality(String) COMMENT 'Gas resource: compute, memory, address_access, state_growth, history, bloom_topics or block_size',
    `share_bucket_pct` UInt8 COMMENT 'Share of the block resource gas this resource took, the lower bound of a 10 percentage point bucket (0 to 90)' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks of at least 10 Mgas in the bucket' CODEC(ZSTD(1)),
    `gas_mgas_sum` Float64 COMMENT 'Sum of block gas used in Mgas' CODEC(ZSTD(1)),
    `p50_ms_sum` Float64 COMMENT 'Sum over blocks of the median engine_newPayload duration across Xatu nodes, in milliseconds. gas_mgas_sum over p50_ms_sum times 1000 is the throughput in Mgas/s' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`, `node_scope`, `client`, `resource`, `share_bucket_pct`)
COMMENT 'engine_newPayload throughput of VALID payloads of at least 10 Mgas per hour, node scope, execution client, gas resource and fixed 10% bucket of the share of block resource gas that resource took';

CREATE TABLE fct_engine_new_payload_resource_share_hourly ON CLUSTER '{cluster}' AS fct_engine_new_payload_resource_share_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_resource_share_hourly_local,
    cityHash64(`hour_start_date_time`, `client`)
);

CREATE TABLE fct_engine_new_payload_gas_bucket_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The UTC day' CODEC(DoubleDelta, ZSTD(1)),
    `node_scope` LowCardinality(String) COMMENT 'Which Xatu nodes the timings come from: reference for reference nodes (node_class eip7870-block-builder), all for every Xatu node',
    `client` LowCardinality(String) COMMENT 'Execution client, lowercased, with go-ethereum as geth',
    `gas_bucket_mgas` UInt16 COMMENT 'Block size bucket, the lower bound in Mgas of a 5 Mgas wide bucket' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks' CODEC(ZSTD(1)),
    `gas_mgas_sum` Float64 COMMENT 'Sum of block gas used in Mgas' CODEC(ZSTD(1)),
    `gas_mgas_sq_sum` Float64 COMMENT 'Sum of squared block gas used in Mgas' CODEC(ZSTD(1)),
    `avg_ms_sum` Float64 COMMENT 'Sum over blocks of the mean engine_newPayload duration across Xatu nodes, in milliseconds' CODEC(ZSTD(1)),
    `p50_ms_sum` Float64 COMMENT 'Sum over blocks of the median engine_newPayload duration across Xatu nodes, in milliseconds' CODEC(ZSTD(1)),
    `p50_ms_sq_sum` Float64 COMMENT 'Sum over blocks of the squared median duration' CODEC(ZSTD(1)),
    `gas_p50_product_sum` Float64 COMMENT 'Sum over blocks of Mgas times the median duration' CODEC(ZSTD(1)),
    `p95_ms_sum` Float64 COMMENT 'Sum over blocks of the 95th percentile engine_newPayload duration across Xatu nodes, in milliseconds' CODEC(ZSTD(1)),
    `p95_ms_sq_sum` Float64 COMMENT 'Sum over blocks of the squared 95th percentile duration' CODEC(ZSTD(1)),
    `gas_p95_product_sum` Float64 COMMENT 'Sum over blocks of Mgas times the 95th percentile duration' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (`day_start_date`, `node_scope`, `client`, `gas_bucket_mgas`)
COMMENT 'engine_newPayload timings of VALID payloads per UTC day, node scope, execution client and 5 Mgas block size bucket, as counts and sums from which a least-squares cost model and throughput can be computed exactly over any range';

CREATE TABLE fct_engine_new_payload_gas_bucket_daily ON CLUSTER '{cluster}' AS fct_engine_new_payload_gas_bucket_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_gas_bucket_daily_local,
    cityHash64(`day_start_date`, `client`)
);

CREATE TABLE fct_engine_new_payload_throughput_histogram_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The UTC day' CODEC(DoubleDelta, ZSTD(1)),
    `node_scope` LowCardinality(String) COMMENT 'Which Xatu nodes the timings come from: reference for reference nodes (node_class eip7870-block-builder), all for every Xatu node',
    `client` LowCardinality(String) COMMENT 'Execution client, lowercased, with go-ethereum as geth',
    `bucket_index` Int16 COMMENT 'Throughput bucket k, covering 1.04^k to 1.04^(k+1) Mgas/s' CODEC(ZSTD(1)),
    `bucket_lower_mgas_per_second` Float32 COMMENT 'Lower bound of the bucket in Mgas/s, inclusive' CODEC(ZSTD(1)),
    `bucket_upper_mgas_per_second` Float32 COMMENT 'Upper bound of the bucket in Mgas/s, exclusive' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks of at least 5 Mgas whose throughput falls in the bucket' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (`day_start_date`, `node_scope`, `client`, `bucket_index`)
COMMENT 'Per-block engine_newPayload throughput histogram (Mgas/s, buckets about 4% wide) of VALID payloads of at least 5 Mgas per UTC day, node scope and execution client';

CREATE TABLE fct_engine_new_payload_throughput_histogram_daily ON CLUSTER '{cluster}' AS fct_engine_new_payload_throughput_histogram_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_throughput_histogram_daily_local,
    cityHash64(`day_start_date`, `client`)
);

CREATE TABLE fct_engine_new_payload_resource_share_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The UTC day' CODEC(DoubleDelta, ZSTD(1)),
    `node_scope` LowCardinality(String) COMMENT 'Which Xatu nodes the timings come from: reference for reference nodes (node_class eip7870-block-builder), all for every Xatu node',
    `client` LowCardinality(String) COMMENT 'Execution client, lowercased, with go-ethereum as geth',
    `resource` LowCardinality(String) COMMENT 'Gas resource: compute, memory, address_access, state_growth, history, bloom_topics or block_size',
    `share_bucket_pct` UInt8 COMMENT 'Share of the block resource gas this resource took, the lower bound of a 10 percentage point bucket (0 to 90)' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks of at least 10 Mgas in the bucket' CODEC(ZSTD(1)),
    `gas_mgas_sum` Float64 COMMENT 'Sum of block gas used in Mgas' CODEC(ZSTD(1)),
    `p50_ms_sum` Float64 COMMENT 'Sum over blocks of the median engine_newPayload duration across Xatu nodes, in milliseconds. gas_mgas_sum over p50_ms_sum times 1000 is the throughput in Mgas/s' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (`day_start_date`, `node_scope`, `client`, `resource`, `share_bucket_pct`)
COMMENT 'engine_newPayload throughput of VALID payloads of at least 10 Mgas per UTC day, node scope, execution client, gas resource and fixed 10% bucket of the share of block resource gas that resource took';

CREATE TABLE fct_engine_new_payload_resource_share_daily ON CLUSTER '{cluster}' AS fct_engine_new_payload_resource_share_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_resource_share_daily_local,
    cityHash64(`day_start_date`, `client`)
);
