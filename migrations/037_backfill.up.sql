CREATE TABLE `${NETWORK_NAME}`.int_backfill_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `event_date_time` DateTime64(3) COMMENT 'When the sentry fetched the beacon block from a beacon node'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(event_date_time)
ORDER BY (`event_date_time`)
COMMENT 'test';

CREATE TABLE `${NETWORK_NAME}`.int_backfill ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_backfill_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_backfill_local,
    cityHash64(
        event_date_time
    )
);
