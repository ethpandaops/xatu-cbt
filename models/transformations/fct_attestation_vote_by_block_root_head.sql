---
table: fct_attestation_vote_by_block_root_head
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - attestation
  - head
dependencies:
  - "{{transformation}}.int_attestation_attested_head"
---
-- Head votes per slot and voted block root, from the real-time head attestations in int_attestation_attested_head.
-- int_attestation_attested_head has one row per (slot, block root, validator), so the count is the number of validators.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    toUInt32(count()) AS vote_count
FROM {{ index .dep "{{transformation}}" "int_attestation_attested_head" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
