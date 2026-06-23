---
table: int_attestation_attested
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
  - "{{transformation}}.int_attestation_attested_canonical"
  - "{{transformation}}.int_attestation_attested_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Canonical-preferred merge of the finalized (canonical) and real-time (head)
-- attestation variants. Canonical values win per (slot, attesting_validator_index,
-- block_root); head-only rows survive so orphaned-target votes the canonical chain
-- never recorded are still represented.
WITH canonical AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        source_epoch,
        source_epoch_start_date_time,
        source_root,
        target_epoch,
        target_epoch_start_date_time,
        target_root,
        block_root,
        attesting_validator_index,
        inclusion_distance
    FROM {{ index .dep "{{transformation}}" "int_attestation_attested_canonical" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

head AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        source_epoch,
        source_epoch_start_date_time,
        source_root,
        target_epoch,
        target_epoch_start_date_time,
        target_root,
        block_root,
        attesting_validator_index,
        propagation_distance
    FROM {{ index .dep "{{transformation}}" "int_attestation_attested_head" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    COALESCE(c.slot, h.slot) AS slot,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.slot_start_date_time ELSE h.slot_start_date_time END AS slot_start_date_time,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.epoch ELSE h.epoch END AS epoch,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.epoch_start_date_time ELSE h.epoch_start_date_time END AS epoch_start_date_time,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.source_epoch ELSE h.source_epoch END AS source_epoch,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.source_epoch_start_date_time ELSE h.source_epoch_start_date_time END AS source_epoch_start_date_time,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.source_root ELSE h.source_root END AS source_root,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.target_epoch ELSE h.target_epoch END AS target_epoch,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.target_epoch_start_date_time ELSE h.target_epoch_start_date_time END AS target_epoch_start_date_time,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.target_root ELSE h.target_root END AS target_root,
    COALESCE(c.block_root, h.block_root) AS block_root,
    COALESCE(c.attesting_validator_index, h.attesting_validator_index) AS attesting_validator_index,
    CASE WHEN c.attesting_validator_index IS NOT NULL THEN c.inclusion_distance ELSE NULL END AS inclusion_distance,
    CASE WHEN h.attesting_validator_index IS NOT NULL THEN h.propagation_distance ELSE NULL END AS propagation_distance
FROM canonical c
GLOBAL FULL OUTER JOIN head h
    ON c.slot = h.slot
    AND c.attesting_validator_index = h.attesting_validator_index
    AND c.block_root = h.block_root
