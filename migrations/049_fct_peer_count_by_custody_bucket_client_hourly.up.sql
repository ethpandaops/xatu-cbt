CREATE TABLE `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_client_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `peer_agent_implementation` LowCardinality(String) COMMENT 'Consensus client implementation' CODEC(ZSTD(1)),
    `custody_bucket` LowCardinality(String) COMMENT 'Custody group count bucket (1-4, 5-8, 9-16, 17-32, 33-64, 65-127, 128)' CODEC(ZSTD(1)),
    `epoch_count` UInt32 COMMENT 'Number of epochs in this hour' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Number of distinct peers for this client and custody bucket in this hour' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (
    hour_start_date_time,
    peer_agent_implementation,
    custody_bucket
)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Peer custody bucket distribution aggregated by client and hour';

CREATE TABLE `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_client_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_client_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_peer_count_by_custody_bucket_client_hourly_local,
    cityHash64(
        hour_start_date_time,
        peer_agent_implementation,
        custody_bucket
    )
);
