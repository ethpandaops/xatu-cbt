---
table: fct_attestation_correctness
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
  - "{{transformation}}.fct_attestation_correctness_canonical"
  - "{{transformation}}.fct_attestation_correctness_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH canonical AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        votes_max,
        votes_head,
        votes_other
    FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_canonical" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

head AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        votes_max,
        votes_head,
        votes_other
    FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_head" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    CASE WHEN c.slot_start_date_time IS NOT NULL AND c.slot_start_date_time != toDateTime(0) THEN c.slot ELSE h.slot END as slot,
    CASE WHEN c.slot_start_date_time IS NOT NULL AND c.slot_start_date_time != toDateTime(0) THEN c.slot_start_date_time ELSE h.slot_start_date_time END as slot_start_date_time,
    CASE WHEN c.slot_start_date_time IS NOT NULL AND c.slot_start_date_time != toDateTime(0) THEN c.epoch ELSE h.epoch END as epoch,
    CASE WHEN c.slot_start_date_time IS NOT NULL AND c.slot_start_date_time != toDateTime(0) THEN c.epoch_start_date_time ELSE h.epoch_start_date_time END as epoch_start_date_time,
    CASE WHEN c.slot_start_date_time IS NOT NULL AND c.slot_start_date_time != toDateTime(0) THEN c.block_root ELSE h.block_root END as block_root,
    CASE WHEN c.slot_start_date_time IS NOT NULL AND c.slot_start_date_time != toDateTime(0) THEN c.votes_max ELSE h.votes_max END as votes_max,
    CASE WHEN c.slot_start_date_time IS NOT NULL AND c.slot_start_date_time != toDateTime(0) THEN c.votes_head ELSE h.votes_head END as votes_head,
    CASE WHEN c.slot_start_date_time IS NOT NULL AND c.slot_start_date_time != toDateTime(0) THEN c.votes_other ELSE h.votes_other END as votes_other
FROM canonical c
GLOBAL FULL OUTER JOIN head h
    ON c.slot_start_date_time = h.slot_start_date_time
    AND cityHash64(coalesce(c.block_root, '')) = cityHash64(coalesce(h.block_root, ''))
SETTINGS join_use_nulls = 1
