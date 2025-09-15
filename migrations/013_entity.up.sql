CREATE TABLE `${NETWORK_NAME}`.int_entity_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),
    `entity` String COMMENT 'The entity of the validator' CODEC(ZSTD(1)),
    `name` Nullable(String) COMMENT 'The name of the validator for the entity' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "institution", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `region` Nullable(String) COMMENT 'The region of the validator' CODEC(ZSTD(1)),
    `consensus_client` Nullable(String) COMMENT 'The consensus client of the validator' CODEC(ZSTD(1)),
    `execution_client` Nullable(String) COMMENT 'The execution client of the validator' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY
    (`validator_index`) COMMENT 'Entity of a validator';

CREATE TABLE `${NETWORK_NAME}`.int_entity ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_entity_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_entity_local,
    cityHash64(`validator_index`)
);