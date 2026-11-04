---
table: fct_epoch_summary_canonical
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 30s"
tags:
  - epoch
  - block
  - attestation
  - canonical
dependencies:
  - "{{transformation}}.int_block_proposer_canonical"
  - "{{transformation}}.fct_attestation_correctness_canonical"
---
-- One row per epoch of the finalized chain: proposer duties, canonical blocks, slots without one, and attestation
-- votes. Each run recounts every epoch its bounds touch in full, so an epoch the previous run cut short is
-- completed by the next one.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    epoch_bounds AS (
        SELECT
            min(epoch_start_date_time) AS min_epoch_start,
            max(epoch_start_date_time) AS max_epoch_start
        FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    slots AS (
        SELECT
            epoch,
            epoch_start_date_time,
            toUInt8(count()) AS slot_count,
            toUInt8(countIf(block_root IS NOT NULL)) AS canonical_block_count
        FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_epoch_start FROM epoch_bounds)
          AND slot_start_date_time < (SELECT max_epoch_start FROM epoch_bounds) + INTERVAL 384 SECOND
        GROUP BY epoch, epoch_start_date_time
    ),
    votes AS (
        SELECT
            epoch,
            toUInt32(sum(votes_max)) AS votes_max,
            toUInt32(sum(ifNull(votes_head, 0))) AS votes_head,
            toUInt32(sum(ifNull(votes_other, 0))) AS votes_other
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_epoch_start FROM epoch_bounds)
          AND slot_start_date_time < (SELECT max_epoch_start FROM epoch_bounds) + INTERVAL 384 SECOND
        GROUP BY epoch
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    s.epoch AS epoch,
    s.epoch_start_date_time AS epoch_start_date_time,
    s.slot_count AS slot_count,
    s.canonical_block_count AS canonical_block_count,
    toUInt8(s.slot_count - s.canonical_block_count) AS missed_slot_count,
    ifNull(v.votes_max, 0) AS votes_max,
    ifNull(v.votes_head, 0) AS votes_head,
    ifNull(v.votes_other, 0) AS votes_other,
    if(ifNull(v.votes_max, 0) > 0, (v.votes_head + v.votes_other) / v.votes_max, NULL) AS participation_rate
FROM slots AS s
GLOBAL LEFT JOIN votes AS v ON s.epoch = v.epoch
SETTINGS join_use_nulls = 1
