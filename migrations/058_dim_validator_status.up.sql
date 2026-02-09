CREATE TABLE `${NETWORK_NAME}`.dim_validator_status_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `version` UInt32 COMMENT 'ReplacingMergeTree version: 4294967295 - epoch, keeps earliest epoch' CODEC(ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),
    `pubkey` String COMMENT 'The public key of the validator' CODEC(ZSTD(1)),

    -- Status
    `status` LowCardinality(String) COMMENT 'Beacon chain validator status (e.g. pending_initialized, active_ongoing)' CODEC(ZSTD(1)),

    -- First observed epoch for this status
    `epoch` UInt32 COMMENT 'First epoch this status was observed' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Wall clock time of the first observed epoch' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator lifecycle fields at the time of first observation
    `activation_epoch` Nullable(UInt64) COMMENT 'Epoch when activation is/was scheduled' CODEC(ZSTD(1)),
    `activation_eligibility_epoch` Nullable(UInt64) COMMENT 'Epoch when validator became eligible for activation' CODEC(ZSTD(1)),
    `exit_epoch` Nullable(UInt64) COMMENT 'Epoch when exit is/was scheduled' CODEC(ZSTD(1)),
    `withdrawable_epoch` Nullable(UInt64) COMMENT 'Epoch when withdrawal becomes possible' CODEC(ZSTD(1)),
    `slashed` Bool COMMENT 'Whether the validator was slashed at this transition'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `version`
) PARTITION BY toStartOfMonth(epoch_start_date_time)
ORDER BY (validator_index, status)
COMMENT 'Validator lifecycle status transitions — one row per (validator_index, status) with the first epoch observed';

CREATE TABLE `${NETWORK_NAME}`.dim_validator_status ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.dim_validator_status_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_validator_status_local,
    cityHash64(validator_index, status)
);
