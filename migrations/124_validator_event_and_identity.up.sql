-- ============================================================================
-- fct_validator_event
-- ============================================================================
CREATE TABLE fct_validator_event_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `version` UInt64 COMMENT 'ReplacingMergeTree version: rows with a known amount win over rows without one, then the newest row wins' CODEC(ZSTD(1)),

    -- Slot dimension (the block that included the operation)
    `slot` UInt32 COMMENT 'Slot of the block that included the operation' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'Wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch of the block that included the operation' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'Root of the block that included the operation' CODEC(ZSTD(1)),

    -- Event
    `validator_index` UInt32 COMMENT 'Index of the validator the event belongs to' CODEC(ZSTD(1)),
    `event_type` LowCardinality(String) COMMENT 'One of deposit, top_up, withdrawal_partial, withdrawal_full, bls_to_execution_change, switch_to_compounding, consolidation_source, consolidation_target, withdrawal_request, exit_request, voluntary_exit, slashed',
    `event_position` UInt64 COMMENT 'Separates events of the same type for a validator in one block: withdrawal index for withdrawals, position in the block for execution requests, 2147483648 plus the order among the pubkey''s deposits in the block for bridge deposits, otherwise 0' CODEC(ZSTD(1)),
    `amount` Nullable(UInt64) COMMENT 'Amount in Gwei: deposit amount, withdrawal amount, partial withdrawal amount accepted into the queue, or balance moved by a consolidation once processed (NULL until then). NULL for other types' CODEC(ZSTD(1)),
    `counterparty_validator_index` Nullable(UInt32) COMMENT 'Other validator: consolidation target (for the source) or source (for the target), or the proposer that included a slashing' CODEC(ZSTD(1)),
    `address` Nullable(String) COMMENT 'Deposit withdrawal credentials, new execution address of a BLS change, withdrawal recipient address, or source address of an execution request' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `version`
) PARTITION BY toStartOfYear(slot_start_date_time)
ORDER BY (validator_index, slot_start_date_time, event_type, event_position)
COMMENT 'Validator lifecycle events: deposits, top-ups, credential changes, consolidations, withdrawals, exit and withdrawal requests, voluntary exits and slashings. Only operations that took effect on chain are included';

CREATE TABLE fct_validator_event ON CLUSTER '{cluster}'
AS fct_validator_event_local
ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_validator_event_local,
    cityHash64(validator_index)
);

-- ============================================================================
-- dim_validator
-- ============================================================================
CREATE TABLE dim_validator_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Snapshot
    `epoch` UInt32 COMMENT 'Epoch of the state this row was taken from' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),

    -- Identity
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),
    `pubkey` String COMMENT 'The public key of the validator' CODEC(ZSTD(1)),
    `withdrawal_credentials` String COMMENT 'Current withdrawal credentials' CODEC(ZSTD(1)),
    `credential_type` LowCardinality(String) COMMENT 'Withdrawal credential prefix: 0x00 (BLS), 0x01 (execution address) or 0x02 (compounding)',
    `withdrawal_address` Nullable(String) COMMENT 'Execution address from 0x01 or 0x02 credentials. NULL for 0x00' CODEC(ZSTD(1)),

    -- State
    `status` LowCardinality(String) COMMENT 'Beacon chain validator status (e.g. pending_queued, active_ongoing, withdrawal_done)',
    `balance` Nullable(UInt64) COMMENT 'Balance in Gwei' CODEC(ZSTD(1)),
    `effective_balance` Nullable(UInt64) COMMENT 'Effective balance in Gwei' CODEC(ZSTD(1)),
    `slashed` Bool COMMENT 'Whether the validator has been slashed',
    `activation_eligibility_epoch` Nullable(UInt64) COMMENT 'Epoch the validator became eligible for activation. NULL if not yet set' CODEC(ZSTD(1)),
    `activation_epoch` Nullable(UInt64) COMMENT 'Epoch the validator activates or activated. NULL if not yet set' CODEC(ZSTD(1)),
    `exit_epoch` Nullable(UInt64) COMMENT 'Epoch the validator exits or exited. NULL if no exit has been initiated' CODEC(ZSTD(1)),
    `withdrawable_epoch` Nullable(UInt64) COMMENT 'Epoch the validator becomes or became withdrawable. NULL if no exit has been initiated' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `epoch`
) PARTITION BY tuple()
ORDER BY (validator_index)
COMMENT 'Current identity and state of every validator, refreshed from the latest epoch';

CREATE TABLE dim_validator ON CLUSTER '{cluster}'
AS dim_validator_local
ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    dim_validator_local,
    cityHash64(validator_index)
);
