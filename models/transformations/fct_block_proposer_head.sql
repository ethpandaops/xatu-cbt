---
table: fct_block_proposer_head
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - proposer
  - head
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_block_gossip"
  - "{{external}}.beacon_api_eth_v1_events_block"
  - - "{{external}}.beacon_api_eth_v1_proposer_duty"
    - "{{external}}.canonical_beacon_proposer_duty"
  - "{{external}}.libp2p_gossipsub_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Proposer duties are deterministic per epoch (fixed by the RANDAO seed
-- epochs in advance), so the head-event stream and the canonical table can
-- never disagree on CONTENT, only on presence. Unioning canonical in makes
-- it a backstop: at the head its range is empty (it lags finalization) and
-- it contributes nothing, while in backfilled history it fills slots whose
-- head-event duty batches were lost to sentry outages. The two duty sources
-- form an OR-group dependency, so forwardfill is not capped at canonical's
-- lag. The outer DISTINCT collapses the union back to one row per slot
-- because the sources' content is identical.
WITH proposer_duties AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        proposer_validator_index,
        proposer_pubkey
    FROM (
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            epoch_start_date_time,
            proposer_validator_index,
            proposer_pubkey
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_proposer_duty" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
        UNION ALL
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            epoch_start_date_time,
            proposer_validator_index,
            proposer_pubkey
        FROM {{ index .dep "{{external}}" "canonical_beacon_proposer_duty" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
    )
),

-- Sentries publish a whole epoch's proposer duties as one batch around the
-- epoch boundary, so the external bound can cover a slot while that batch is
-- still being ingested. Processing such a slot bakes the gap in permanently:
-- the slot loses its proposer pubkey, and a missed slot in the gap vanishes
-- from the table entirely (nothing on either side of the join). Every slot
-- has exactly one proposer, so completeness here is simply presence: require
-- every slot on the 12s grid inside the interval to have a duty row in the
-- union of both sources, and fail the task otherwise so the scheduler
-- retries once ingestion has settled.
--
-- These scans deliberately read the source tables directly (not
-- proposer_duties) and skip FINAL: duty PRESENCE is unaffected by
-- ReplacingMergeTree version replacement.
--
-- Bounds are not necessarily aligned to the 12s grid (backfill chunks are
-- arbitrary spans whose length need not be a multiple of 12), so the
-- expected count cannot be derived from the span length alone: snap the
-- start bound up to the first on-grid timestamp, using the grid phase
-- observed from any ingested slot in the interval, then count grid points
-- up to the end bound. With zero ingested slots the phase is unknowable,
-- so keep the span-derived over-estimate: the task fails and retries,
-- the desired outcome for an interval with no duty data at all.
duty_slots AS (
    SELECT
        slot,
        any(slot_ts) AS slot_ts
    FROM (
        SELECT slot, toUnixTimestamp(slot_start_date_time) AS slot_ts
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_proposer_duty" "helpers" "from" }}
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
        UNION ALL
        SELECT slot, toUnixTimestamp(slot_start_date_time) AS slot_ts
        FROM {{ index .dep "{{external}}" "canonical_beacon_proposer_duty" "helpers" "from" }}
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
    )
    GROUP BY slot
),

gate AS (
    SELECT
        (
            SELECT
                if(count() = 0,
                   toInt64(intDiv({{ .bounds.end }} - {{ .bounds.start }}, 12) + 1),
                   toInt64(intDiv(
                       {{ .bounds.end }}
                           - ({{ .bounds.start }} + ((toInt64(any(slot_ts)) - {{ .bounds.start }}) % 12)),
                       12) + 1))
            FROM duty_slots
        ) - toInt64((SELECT count() FROM duty_slots)) AS missing_slots
),

block_gossip AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block as block_root
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_block_gossip" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

block_events AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block as block_root
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

gossipsub_blocks AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block as block_root,
        proposer_index
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

all_blocks AS (
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        NULL as proposer_index
    FROM block_gossip
    
    UNION ALL
    
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        NULL as proposer_index
    FROM block_events
    
    UNION ALL
    
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        proposer_index
    FROM gossipsub_blocks
),

deduplicated_blocks AS (
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        argMax(proposer_index, proposer_index IS NOT NULL) as proposer_index
    FROM all_blocks
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    COALESCE(pd.slot, db.slot) as slot,
    COALESCE(pd.slot_start_date_time, db.slot_start_date_time) as slot_start_date_time,
    COALESCE(pd.epoch, db.epoch) as epoch,
    COALESCE(pd.epoch_start_date_time, db.epoch_start_date_time) as epoch_start_date_time,
    COALESCE(pd.proposer_validator_index, db.proposer_index) as proposer_validator_index,
    pd.proposer_pubkey as proposer_pubkey,
    NULLIF(db.block_root, '') AS block_root
FROM proposer_duties pd
GLOBAL FULL OUTER JOIN deduplicated_blocks db ON pd.slot = db.slot
-- The gate protects against ingestion RACES, which settle within seconds
-- (emit-to-queryable p999 is under 30s). An interval whose end is more than
-- an hour old is settled: a slot absent from BOTH the head-event stream and
-- the canonical table by then is permanently absent, and refusing it forever
-- would wedge backfill at the first such gap. For settled intervals, bake
-- what exists.
WHERE (
    SELECT throwIf(
        missing_slots > 0
            AND fromUnixTimestamp({{ .bounds.end }}) > now() - INTERVAL 1 HOUR,
        'fct_block_proposer_head: interval has slots with no proposer duty (ingestion not settled), failing task for retry'
    )
    FROM gate
) = 0
SETTINGS join_use_nulls = 1;
