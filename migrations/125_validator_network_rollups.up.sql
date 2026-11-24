-- ============================================================================
-- fct_validator_count_by_credential_type_daily
-- ============================================================================
CREATE TABLE fct_validator_count_by_credential_type_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch of the snapshot: the last epoch of the day, or the latest epoch for today' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Wall clock time when the snapshot epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `credential_type` LowCardinality(String) COMMENT 'Withdrawal credential prefix at the snapshot: 0x00, 0x01 or 0x02',
    `validator_count` UInt32 COMMENT 'Validators in the beacon state with this credential type, in any status' CODEC(ZSTD(1)),
    `active_validator_count` UInt32 COMMENT 'Active validators (active_ongoing, active_exiting, active_slashed)' CODEC(ZSTD(1)),
    `active_effective_balance` UInt64 COMMENT 'Sum of active validators'' effective balance in Gwei (active stake)' CODEC(ZSTD(1)),
    `active_balance` UInt64 COMMENT 'Sum of active validators'' balance in Gwei' CODEC(ZSTD(1)),
    `pending_validator_count` UInt32 COMMENT 'Validators waiting for activation (pending_initialized, pending_queued)' CODEC(ZSTD(1)),
    `exiting_validator_count` UInt32 COMMENT 'Active validators with an exit started (active_exiting, active_slashed)' CODEC(ZSTD(1)),
    `exited_validator_count` UInt32 COMMENT 'Exited validators (exited_unslashed, exited_slashed, withdrawal_possible, withdrawal_done)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `epoch`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (day_start_date, credential_type)
COMMENT 'Validators and stake by withdrawal credential type, one snapshot per day';

CREATE TABLE fct_validator_count_by_credential_type_daily ON CLUSTER '{cluster}'
AS fct_validator_count_by_credential_type_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_validator_count_by_credential_type_daily_local, cityHash64(day_start_date));

-- ============================================================================
-- fct_validator_effective_balance_by_credential_type_daily
-- ============================================================================
CREATE TABLE fct_validator_effective_balance_by_credential_type_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch of the snapshot: the last epoch of the day, or the latest epoch for today' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Wall clock time when the snapshot epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `credential_type` LowCardinality(String) COMMENT 'Withdrawal credential prefix at the snapshot: 0x00, 0x01 or 0x02',
    `effective_balance_eth` UInt32 COMMENT 'Effective balance in whole ETH (effective balance moves in 1 ETH steps)' CODEC(ZSTD(1)),
    `validator_count` UInt32 COMMENT 'Active validators with this credential type and effective balance' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `epoch`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (day_start_date, credential_type, effective_balance_eth)
COMMENT 'Histogram of active validators'' effective balance by withdrawal credential type, one snapshot per day';

CREATE TABLE fct_validator_effective_balance_by_credential_type_daily ON CLUSTER '{cluster}'
AS fct_validator_effective_balance_by_credential_type_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_validator_effective_balance_by_credential_type_daily_local, cityHash64(day_start_date));

-- ============================================================================
-- fct_validator_stake_by_entity_daily
-- ============================================================================
CREATE TABLE fct_validator_stake_by_entity_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch of the snapshot: the last epoch of the day, or the latest epoch for today' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Wall clock time when the snapshot epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `entity` String COMMENT 'Entity label from dim_node as it is today, or unknown' CODEC(ZSTD(1)),
    `credential_type` LowCardinality(String) COMMENT 'Withdrawal credential prefix at the snapshot: 0x00, 0x01 or 0x02',
    `active_validator_count` UInt32 COMMENT 'Active validators (active_ongoing, active_exiting, active_slashed)' CODEC(ZSTD(1)),
    `active_effective_balance` UInt64 COMMENT 'Sum of active validators'' effective balance in Gwei (active stake)' CODEC(ZSTD(1)),
    `active_balance` UInt64 COMMENT 'Sum of active validators'' balance in Gwei' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `epoch`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (day_start_date, entity, credential_type)
COMMENT 'Active validators and stake by entity and withdrawal credential type, one snapshot per day';

CREATE TABLE fct_validator_stake_by_entity_daily ON CLUSTER '{cluster}'
AS fct_validator_stake_by_entity_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_validator_stake_by_entity_daily_local, cityHash64(day_start_date));

-- ============================================================================
-- fct_validator_queue_daily
-- ============================================================================
CREATE TABLE fct_validator_queue_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch of the snapshot: the last epoch of the day, or the latest epoch for today' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Wall clock time when the snapshot epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `pending_initialized_count` UInt32 COMMENT 'Validators with status pending_initialized (deposited, not yet eligible for activation)' CODEC(ZSTD(1)),
    `pending_initialized_balance` UInt64 COMMENT 'Balance of pending_initialized validators in Gwei' CODEC(ZSTD(1)),
    `activation_queue_count` UInt32 COMMENT 'Validators with status pending_queued (eligible, waiting for activation)' CODEC(ZSTD(1)),
    `activation_queue_effective_balance` UInt64 COMMENT 'Effective balance of pending_queued validators in Gwei' CODEC(ZSTD(1)),
    `exit_queue_count` UInt32 COMMENT 'Validators with an exit started but not yet exited (active_exiting, active_slashed)' CODEC(ZSTD(1)),
    `exit_queue_effective_balance` UInt64 COMMENT 'Effective balance of validators in the exit queue in Gwei' CODEC(ZSTD(1)),
    `pending_deposit_count` UInt32 COMMENT 'Entries in the pending deposit queue (Electra). 0 before Electra' CODEC(ZSTD(1)),
    `pending_deposit_amount` UInt64 COMMENT 'Amount in the pending deposit queue in Gwei' CODEC(ZSTD(1)),
    `pending_consolidation_count` UInt32 COMMENT 'Entries in the pending consolidation queue (Electra). 0 before Electra' CODEC(ZSTD(1)),
    `pending_consolidation_source_effective_balance` UInt64 COMMENT 'Effective balance of the queued consolidations'' source validators in Gwei' CODEC(ZSTD(1)),
    `pending_partial_withdrawal_count` UInt32 COMMENT 'Entries in the pending partial withdrawal queue (Electra). 0 before Electra' CODEC(ZSTD(1)),
    `pending_partial_withdrawal_amount` UInt64 COMMENT 'Amount in the pending partial withdrawal queue in Gwei' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `epoch`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Activation and exit queues and the Electra pending queues, one snapshot per day';

CREATE TABLE fct_validator_queue_daily ON CLUSTER '{cluster}'
AS fct_validator_queue_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_validator_queue_daily_local, cityHash64(day_start_date));

-- ============================================================================
-- fct_validator_operation_daily
-- ============================================================================
CREATE TABLE fct_validator_operation_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC) of the blocks that included the operations' CODEC(DoubleDelta, ZSTD(1)),
    `operation` LowCardinality(String) COMMENT 'One of deposit, top_up, withdrawal_partial, withdrawal_full, bls_to_execution_change, switch_to_compounding, consolidation, withdrawal_request, exit_request, voluntary_exit, slashing',
    `operation_count` UInt32 COMMENT 'Number of operations that took effect' CODEC(ZSTD(1)),
    `validator_count` UInt32 COMMENT 'Distinct validators (pubkeys for deposits and top-ups)' CODEC(ZSTD(1)),
    `amount` Nullable(UInt64) COMMENT 'Gwei deposited, withdrawn, or accepted into the partial withdrawal queue. NULL for operations without an amount' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (day_start_date, operation)
COMMENT 'Daily counts and amounts of validator operations that took effect';

CREATE TABLE fct_validator_operation_daily ON CLUSTER '{cluster}'
AS fct_validator_operation_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_validator_operation_daily_local, cityHash64(day_start_date));

-- ============================================================================
-- fct_attestation_reward_by_validator_daily
-- ============================================================================
CREATE TABLE fct_attestation_reward_by_validator_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),
    `epoch_count` UInt32 COMMENT 'Epochs of the day the validator was active (had an attestation reward)' CODEC(ZSTD(1)),
    `head` Int64 COMMENT 'Head vote reward in Gwei' CODEC(ZSTD(1)),
    `target` Int64 COMMENT 'Target vote reward in Gwei (negative when penalised)' CODEC(ZSTD(1)),
    `source` Int64 COMMENT 'Source vote reward in Gwei (negative when penalised)' CODEC(ZSTD(1)),
    `inactivity` Int64 COMMENT 'Inactivity penalty in Gwei' CODEC(ZSTD(1)),
    `total` Int64 COMMENT 'head + target + source + inactivity in Gwei' CODEC(ZSTD(1)),
    `penalty` Int64 COMMENT 'Sum of the negative components in Gwei' CODEC(ZSTD(1)),
    `penalized_epoch_count` UInt32 COMMENT 'Epochs with any negative component' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (validator_index, day_start_date)
COMMENT 'Attestation rewards and penalties per validator per day';

CREATE TABLE fct_attestation_reward_by_validator_daily ON CLUSTER '{cluster}'
AS fct_attestation_reward_by_validator_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_attestation_reward_by_validator_daily_local, cityHash64(validator_index, day_start_date));

-- ============================================================================
-- fct_sync_committee_reward_by_validator_daily
-- ============================================================================
CREATE TABLE fct_sync_committee_reward_by_validator_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Blocks of the day with a sync committee reward for the validator' CODEC(ZSTD(1)),
    `total` Int64 COMMENT 'Net sync committee reward in Gwei' CODEC(ZSTD(1)),
    `reward` Int64 COMMENT 'Sum of positive rewards in Gwei' CODEC(ZSTD(1)),
    `penalty` Int64 COMMENT 'Sum of negative rewards (missed votes) in Gwei' CODEC(ZSTD(1)),
    `penalized_slot_count` UInt32 COMMENT 'Blocks with a negative reward' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (validator_index, day_start_date)
COMMENT 'Sync committee rewards and penalties per validator per day';

CREATE TABLE fct_sync_committee_reward_by_validator_daily ON CLUSTER '{cluster}'
AS fct_sync_committee_reward_by_validator_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_sync_committee_reward_by_validator_daily_local, cityHash64(validator_index, day_start_date));

-- ============================================================================
-- fct_block_proposer_reward
-- ============================================================================
CREATE TABLE fct_block_proposer_reward_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The canonical block root' CODEC(ZSTD(1)),
    `proposer_index` UInt32 COMMENT 'The validator index of the proposer' CODEC(ZSTD(1)),
    `total` UInt64 COMMENT 'Total consensus layer proposer reward in Gwei' CODEC(ZSTD(1)),
    `attestations` UInt64 COMMENT 'Reward for included attestations in Gwei' CODEC(ZSTD(1)),
    `sync_aggregate` UInt64 COMMENT 'Reward for the included sync aggregate in Gwei' CODEC(ZSTD(1)),
    `proposer_slashings` UInt64 COMMENT 'Reward for included proposer slashings in Gwei' CODEC(ZSTD(1)),
    `attester_slashings` UInt64 COMMENT 'Reward for included attester slashings in Gwei' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (proposer_index, slot_start_date_time)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Consensus layer reward of each canonical block''s proposer';

CREATE TABLE fct_block_proposer_reward ON CLUSTER '{cluster}'
AS fct_block_proposer_reward_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_block_proposer_reward_local, cityHash64(proposer_index, slot_start_date_time));

ALTER TABLE fct_block_proposer_reward_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot_start_date_time
(
    SELECT *
    ORDER BY (slot_start_date_time, proposer_index)
);

-- ============================================================================
-- fct_validator_reward_total_daily
-- ============================================================================
CREATE TABLE fct_validator_reward_total_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The day (UTC)' CODEC(DoubleDelta, ZSTD(1)),
    `attesting_validator_count` UInt32 COMMENT 'Validators with an attestation reward on the day' CODEC(ZSTD(1)),
    `attestation_head` Int64 COMMENT 'Head vote rewards in Gwei' CODEC(ZSTD(1)),
    `attestation_target` Int64 COMMENT 'Target vote rewards in Gwei' CODEC(ZSTD(1)),
    `attestation_source` Int64 COMMENT 'Source vote rewards in Gwei' CODEC(ZSTD(1)),
    `attestation_inactivity` Int64 COMMENT 'Inactivity penalties in Gwei' CODEC(ZSTD(1)),
    `attestation_total` Int64 COMMENT 'Net attestation rewards in Gwei' CODEC(ZSTD(1)),
    `attestation_penalty` Int64 COMMENT 'Negative attestation components in Gwei' CODEC(ZSTD(1)),
    `sync_committee_reward` Int64 COMMENT 'Positive sync committee rewards in Gwei' CODEC(ZSTD(1)),
    `sync_committee_penalty` Int64 COMMENT 'Sync committee penalties in Gwei' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Canonical blocks on the day' CODEC(ZSTD(1)),
    `proposer_total` UInt64 COMMENT 'Consensus layer proposer rewards in Gwei' CODEC(ZSTD(1)),
    `proposer_attestations` UInt64 COMMENT 'Proposer rewards for attestations in Gwei' CODEC(ZSTD(1)),
    `proposer_sync_aggregate` UInt64 COMMENT 'Proposer rewards for sync aggregates in Gwei' CODEC(ZSTD(1)),
    `proposer_slashings` UInt64 COMMENT 'Proposer rewards for slashings in Gwei' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Network-wide consensus layer rewards and penalties per day';

CREATE TABLE fct_validator_reward_total_daily ON CLUSTER '{cluster}'
AS fct_validator_reward_total_daily_local
ENGINE = Distributed('{cluster}', currentDatabase(), fct_validator_reward_total_daily_local, cityHash64(day_start_date));
