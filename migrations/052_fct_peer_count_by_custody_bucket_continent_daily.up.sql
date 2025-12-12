CREATE TABLE `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_continent_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start date of the day' CODEC(DoubleDelta, ZSTD(1)),
    `peer_geo_continent_code` LowCardinality(String) COMMENT 'Continent code of the peer' CODEC(ZSTD(1)),
    `custody_bucket` LowCardinality(String) COMMENT 'Custody group count bucket (1-4, 5-8, 9-16, 17-32, 33-64, 65-127, 128)' CODEC(ZSTD(1)),
    `epoch_count` UInt32 COMMENT 'Number of epochs in this day' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Number of distinct peers in this continent and custody bucket in this day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY toYYYYMM(day_start_date)
ORDER BY (
    day_start_date,
    peer_geo_continent_code,
    custody_bucket
)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Peer custody bucket distribution aggregated by continent and day';

CREATE TABLE `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_continent_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_peer_count_by_custody_bucket_continent_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_peer_count_by_custody_bucket_continent_daily_local,
    cityHash64(
        day_start_date,
        peer_geo_continent_code,
        custody_bucket
    )
);
