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
  - "{{external}}.beacon_api_eth_v1_events_attestation"
  - "{{external}}.libp2p_gossipsub_beacon_attestation"
  - "{{transformation}}.int_attestation_first_seen_aggregate"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- One row per (slot, validator, vote) with raw and aggregate first-seen times.
-- FULL OUTER JOIN on the vote key preserves slashable double votes as separate rows.
WITH raw AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        attesting_validator_index AS validator_index,
        attesting_validator_committee_index AS committee_index,
        beacon_block_root AS block_root,
        source_epoch,
        source_root,
        target_epoch,
        target_root,
        MIN(propagation_slot_start_diff) AS seen,
        argMin(source, propagation_slot_start_diff) AS source
    FROM (
        SELECT 'beacon_api_eth_v1_events_attestation' AS source,
               slot, slot_start_date_time, epoch, epoch_start_date_time,
               attesting_validator_index, attesting_validator_committee_index,
               beacon_block_root, source_epoch, source_root, target_epoch, target_root,
               propagation_slot_start_diff
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_attestation" "helpers" "from" }}
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
            AND aggregation_bits = ''
            AND attesting_validator_index IS NOT NULL

        UNION ALL

        SELECT 'libp2p_gossipsub_beacon_attestation' AS source,
               slot, slot_start_date_time, epoch, epoch_start_date_time,
               attesting_validator_index, attesting_validator_committee_index,
               beacon_block_root, source_epoch, source_root, target_epoch, target_root,
               propagation_slot_start_diff
        FROM {{ index .dep "{{external}}" "libp2p_gossipsub_beacon_attestation" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
            AND aggregation_bits = ''
            AND attesting_validator_index IS NOT NULL
    )
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time,
             attesting_validator_index, attesting_validator_committee_index,
             beacon_block_root, source_epoch, source_root, target_epoch, target_root
),
agg AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        attesting_validator_index AS validator_index,
        committee_index,
        block_root,
        source_epoch,
        source_root,
        target_epoch,
        target_root,
        seen_slot_start_diff AS seen,
        source
    FROM {{ index .dep "{{transformation}}" "int_attestation_first_seen_aggregate" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    coalesce(r.slot, a.slot) AS slot,
    coalesce(r.slot_start_date_time, a.slot_start_date_time) AS slot_start_date_time,
    coalesce(r.epoch, a.epoch) AS epoch,
    coalesce(r.epoch_start_date_time, a.epoch_start_date_time) AS epoch_start_date_time,
    coalesce(r.validator_index, a.validator_index) AS validator_index,
    coalesce(r.committee_index, a.committee_index) AS committee_index,
    coalesce(r.block_root, a.block_root) AS block_root,
    coalesce(r.source_epoch, a.source_epoch) AS source_epoch,
    coalesce(r.source_root, a.source_root) AS source_root,
    coalesce(r.target_epoch, a.target_epoch) AS target_epoch,
    coalesce(r.target_root, a.target_root) AS target_root,
    r.seen AS raw_seen_slot_start_diff,
    ifNull(r.source, '') AS raw_source,
    a.seen AS agg_seen_slot_start_diff,
    ifNull(a.source, '') AS agg_source
FROM raw r
FULL OUTER JOIN agg a
    ON r.slot_start_date_time = a.slot_start_date_time
   AND r.validator_index = a.validator_index
   AND r.block_root = a.block_root
   AND r.source_epoch = a.source_epoch
   AND r.source_root = a.source_root
   AND r.target_epoch = a.target_epoch
   AND r.target_root = a.target_root
SETTINGS join_use_nulls = 1
