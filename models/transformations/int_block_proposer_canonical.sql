---
table: int_block_proposer_canonical
interval:
  max: 5000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - proposer
  - canonical
dependencies:
  - "{{external}}.canonical_beacon_block"
  - "{{external}}.canonical_beacon_proposer_duty"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH proposer_duties AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        proposer_validator_index,
        proposer_pubkey
    FROM `{{ index .dep "{{external}}" "canonical_beacon_proposer_duty" "database" }}`.`canonical_beacon_proposer_duty` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

blocks AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        proposer_index
    FROM `{{ index .dep "{{external}}" "canonical_beacon_block" "database" }}`.`canonical_beacon_block` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)

SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    COALESCE(pd.slot, b.slot) as slot,
    COALESCE(pd.slot_start_date_time, b.slot_start_date_time) as slot_start_date_time,
    COALESCE(pd.epoch, b.epoch) as epoch,
    COALESCE(pd.epoch_start_date_time, b.epoch_start_date_time) as epoch_start_date_time,
    COALESCE(pd.proposer_validator_index, b.proposer_index) as proposer_validator_index,
    pd.proposer_pubkey as proposer_pubkey,
    CASE WHEN b.block_root = '' THEN NULL ELSE b.block_root END as block_root
FROM proposer_duties pd
FULL OUTER JOIN blocks b ON pd.slot = b.slot;
