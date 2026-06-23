---
table: fct_attestation_correctness_by_validator
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
  - canonical
dependencies:
  - "{{transformation}}.fct_attestation_correctness_by_validator_canonical"
  - "{{transformation}}.fct_attestation_correctness_by_validator_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH canonical AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        attesting_validator_index,
        block_root,
        slot_distance,
        inclusion_distance,
        `status`
    FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_by_validator_canonical" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

head AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        attesting_validator_index,
        block_root,
        slot_distance,
        propagation_distance
    FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_by_validator_head" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)

-- Canonical-preferred merge: one row per (slot_start_date_time, attesting_validator_index).
-- Prefer the canonical (finalized) value when present, otherwise fall back to the head value.
-- propagation_distance is only available from head; inclusion_distance and status only from canonical.
SELECT
  fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
  CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.slot ELSE h.slot END AS slot,
  CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.slot_start_date_time ELSE h.slot_start_date_time END AS slot_start_date_time,
  CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.epoch ELSE h.epoch END AS epoch,
  CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.epoch_start_date_time ELSE h.epoch_start_date_time END AS epoch_start_date_time,
  CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.attesting_validator_index ELSE h.attesting_validator_index END AS attesting_validator_index,
  CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.block_root ELSE h.block_root END AS block_root,
  CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.slot_distance ELSE h.slot_distance END AS slot_distance,
  h.propagation_distance AS propagation_distance,
  c.inclusion_distance AS inclusion_distance,
  c.`status` AS `status`
FROM canonical c
GLOBAL FULL OUTER JOIN head h ON
    c.slot_start_date_time = h.slot_start_date_time
    AND c.attesting_validator_index = h.attesting_validator_index
SETTINGS join_use_nulls = 1
