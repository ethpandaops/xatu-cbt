---
database: mainnet
table: int_slot__block_proposer_canonical
interval:
  max: 5000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - mainnet
  - slot
  - block
  - proposer
  - canonical
dependencies:
  - mainnet.canonical_beacon_block
  - mainnet.canonical_beacon_proposer_duty
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
    FROM mainnet.canonical_beacon_proposer_duty
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
    FROM mainnet.canonical_beacon_block
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

-- Delete old rows
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});
