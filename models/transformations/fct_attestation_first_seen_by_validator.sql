---
table: fct_attestation_first_seen_by_validator
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - attestation
  - validator
dependencies:
  - "{{transformation}}.int_attestation_first_seen"
  - "{{transformation}}.int_attestation_first_seen_aggregate"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- FULL OUTER JOIN so a validator appears if seen via EITHER raw or aggregate (or both).
-- Raw-only = validator's unaggregated attestation was seen but the aggregate form wasn't yet.
-- Agg-only = we never saw the single attestation, only the aggregate that included them.
WITH raw AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        attesting_validator_index,
        attesting_validator_committee_index AS committee_index,
        seen_slot_start_diff,
        source,
        block_root,
        source_epoch,
        source_root,
        target_epoch,
        target_root
    FROM {{ index .dep "{{transformation}}" "int_attestation_first_seen" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        -- Hard-fail if any raw row predates migration 077 (source/target roots not yet
        -- backfilled). Silently filtering would let CBT mark this range complete and never
        -- recover. Throwing forces the task to fail until int_attestation_first_seen is
        -- re-processed for the range, at which point CBT retries and this passes.
        AND throwIf(
            source_root = '' OR target_root = '',
            'int_attestation_first_seen has rows with empty source_root/target_root in this range (pre-migration-077 data). Re-process int_attestation_first_seen before running fct_attestation_first_seen_by_validator.'
        ) = 0
),
agg AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        attesting_validator_index,
        committee_index,
        seen_slot_start_diff,
        source,
        block_root,
        source_epoch,
        source_root,
        target_epoch,
        target_root
    FROM {{ index .dep "{{transformation}}" "int_attestation_first_seen_aggregate" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    coalesce(r.slot, a.slot) AS slot,
    coalesce(r.slot_start_date_time, a.slot_start_date_time) AS slot_start_date_time,
    coalesce(r.epoch, a.epoch) AS epoch,
    coalesce(r.epoch_start_date_time, a.epoch_start_date_time) AS epoch_start_date_time,
    coalesce(r.attesting_validator_index, a.attesting_validator_index) AS validator_index,
    coalesce(r.committee_index, a.committee_index) AS committee_index,
    r.seen_slot_start_diff AS raw_seen_slot_start_diff,
    ifNull(r.source, '') AS raw_source,
    ifNull(r.block_root, '') AS raw_block_root,
    ifNull(r.source_epoch, 0) AS raw_source_epoch,
    ifNull(r.source_root, '') AS raw_source_root,
    ifNull(r.target_epoch, 0) AS raw_target_epoch,
    ifNull(r.target_root, '') AS raw_target_root,
    a.seen_slot_start_diff AS agg_seen_slot_start_diff,
    ifNull(a.source, '') AS agg_source,
    ifNull(a.block_root, '') AS agg_block_root,
    ifNull(a.source_epoch, 0) AS agg_source_epoch,
    ifNull(a.source_root, '') AS agg_source_root,
    ifNull(a.target_epoch, 0) AS agg_target_epoch,
    ifNull(a.target_root, '') AS agg_target_root
FROM raw r
FULL OUTER JOIN agg a
    ON r.slot_start_date_time = a.slot_start_date_time
   AND r.attesting_validator_index = a.attesting_validator_index
SETTINGS join_use_nulls = 1
