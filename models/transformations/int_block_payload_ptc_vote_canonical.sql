---
table: int_block_payload_ptc_vote_canonical
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - payload
  - ptc
  - epbs
  - canonical
dependencies:
  - "{{external}}.canonical_beacon_block_payload_attestation"
  - "{{transformation}}.int_block_canonical"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas (ePBS): PTC payload attestations included in canonical blocks,
-- re-keyed to the ATTESTED block. A block at slot N carries up to 4 aggregated
-- payload attestations whose beacon_block_root is its parent (attested slot is
-- always N-1, spec-enforced), so the containing-block scan extends one slot
-- past the interval end while output rows stay keyed inside the interval by
-- the attested block's own slot. Aggregates are packed per vote combination
-- with disjoint bitvectors, so attesting_validator_count sums cleanly.
WITH included_attestations AS (
    SELECT
        slot AS included_in_slot,
        block_root AS included_in_block_root,
        block_version,
        beacon_block_root,
        position,
        payload_present,
        blob_data_available,
        attesting_validator_count
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_payload_attestation" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }} + 12)
        AND meta_network_name = '{{ .env.NETWORK }}'
),
attested_blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root
    FROM {{ index .dep "{{transformation}}" "int_block_canonical" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    b.slot AS slot,
    b.slot_start_date_time AS slot_start_date_time,
    b.epoch AS epoch,
    b.epoch_start_date_time AS epoch_start_date_time,
    b.block_root AS block_root,
    any(a.block_version) AS block_version,
    any(a.included_in_slot) AS included_in_slot,
    any(a.included_in_block_root) AS included_in_block_root,
    SUM(a.attesting_validator_count) AS ptc_validators,
    sumIf(a.attesting_validator_count, a.payload_present) AS payload_present_votes,
    sumIf(a.attesting_validator_count, a.blob_data_available) AS blob_data_available_votes
FROM included_attestations a
GLOBAL INNER JOIN attested_blocks b ON a.beacon_block_root = b.block_root
GROUP BY b.slot, b.slot_start_date_time, b.epoch, b.epoch_start_date_time, b.block_root
