CREATE TABLE dim_blob_submitter_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The blob submitter address (L1 batcher account)' CODEC(ZSTD(1)),
    `name` Nullable(String) COMMENT 'Resolved name of the blob submitter (e.g. rollup name)' CODEC(ZSTD(1)),
    `labels` Array(String) COMMENT 'Category labels for the submitter (e.g. rollup, settlement)' CODEC(ZSTD(1)),
    `sources` Array(String) COMMENT 'Data sources that contributed to this record (e.g. dune, growthepie, eth-labels)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`address`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Blob submitter address to name mapping, refreshed from external label sources (Dune, growthepie, eth-labels).';

CREATE TABLE dim_blob_submitter ON CLUSTER '{cluster}' AS dim_blob_submitter_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    dim_blob_submitter_local,
    cityHash64(`address`)
);
