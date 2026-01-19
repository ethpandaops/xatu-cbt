CREATE TABLE `${NETWORK_NAME}`.dim_function_signature_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `selector` String COMMENT 'Function selector (first 4 bytes of keccak256 hash, hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `name` String COMMENT 'Function signature name (e.g., transfer(address,uint256))' CODEC(ZSTD(1)),
    `has_verified_contract` Bool DEFAULT false COMMENT 'Whether this signature comes from a verified contract source' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PRIMARY KEY (selector)
ORDER BY (selector)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Function signature lookup table populated from Sourcify signature database.';

CREATE TABLE `${NETWORK_NAME}`.dim_function_signature ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.dim_function_signature_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_function_signature_local,
    cityHash64(selector)
);
