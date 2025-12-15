CREATE TABLE `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_monthly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `month_start_date` Date COMMENT 'Start date of the month' CODEC(DoubleDelta, ZSTD(1)),
    `custody_bucket` LowCardinality(String) COMMENT 'Custody group count bucket (1-4, 5-8, 9-16, 17-32, 33-64, 65-127, 128)' CODEC(ZSTD(1)),
    `epoch_count` UInt32 COMMENT 'Number of epochs in this month' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Number of distinct peers in this custody bucket in this month' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY toYear(month_start_date)
ORDER BY (
    month_start_date,
    custody_bucket
)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Peer custody bucket distribution aggregated by month';

CREATE TABLE `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_monthly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_monthly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_peer_count_by_custody_bucket_monthly_local,
    cityHash64(
        month_start_date,
        custody_bucket
    )
);
