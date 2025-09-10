CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `votes_max` UInt32 COMMENT 'The maximum number of scheduled votes for the block' CODEC(ZSTD(1)),
    `votes_actual` UInt32 COMMENT 'The number of actual votes for the block' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(epoch, 30000)
ORDER BY
    (`slot`, `block_root`) COMMENT 'Attestation correctness of a block for the unfinalized chain. Forks in the chain may cause multiple block roots for the same slot to be present';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_correctness_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_correctness_head_local,
    cityHash64(`slot`, `block_root`)
);

