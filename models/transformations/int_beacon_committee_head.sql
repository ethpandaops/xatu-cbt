---
table: int_beacon_committee_head
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - beacon
  - committee
  - head
dependencies:
  - "{{external}}.beacon_api_eth_v1_beacon_committee"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
per_view AS
(
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        committee_index,
        arraySort(arrayDistinct(validators)) AS v_norm,
        meta_client_name AS client
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_committee" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

votes AS
(
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        committee_index,
        v_norm,
        uniqExact(client) AS votes
    FROM per_view
    GROUP BY
        slot, slot_start_date_time, epoch, epoch_start_date_time,
        committee_index, v_norm
),

-- Sentries publish a whole epoch's committees as one batch around the epoch
-- boundary, so the external bound can cover a slot while that batch is still
-- being ingested. Processing such a slot bakes in a partial committee set
-- permanently, as incremental intervals are never reprocessed. The committee
-- count per slot is constant within an epoch, so require every slot in the
-- interval to have as many committees as its epoch shows anywhere in a
-- +-2 epoch window, and fail the task otherwise so the scheduler retries
-- once ingestion has settled.
--
-- These scans deliberately read the source table directly (not per_view) and
-- skip FINAL: committee PRESENCE is unaffected by ReplacingMergeTree version
-- replacement, and reusing per_view would inline its heavy validators column
-- a second time.
expected_per_epoch AS
(
    SELECT
        epoch,
        uniqExact(committee_index) AS expected_committees
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_committee" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 768 SECOND
                                   AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 768 SECOND
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY epoch
),

slot_committees AS
(
    SELECT
        slot,
        any(epoch) AS epoch,
        any(toUnixTimestamp(slot_start_date_time)) AS slot_ts,
        uniqExact(committee_index) AS committees
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_committee" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot
),

-- A slot still mid-ingest can also have ZERO visible rows, which no per-slot
-- comparison can see, so additionally require every slot on the 12s grid
-- inside the interval to be present at all. Slot duration is hardcoded the
-- same way other models hardcode it (e.g. the /12000 propagation buckets).
--
-- Bounds are not necessarily aligned to that grid (backfill chunks are
-- arbitrary spans whose length need not be a multiple of 12), so the
-- expected count cannot be derived from the span length alone: snap the
-- start bound up to the first on-grid timestamp, using the grid phase
-- observed from any ingested slot in the interval, then count grid points
-- up to the end bound. With zero ingested slots the phase is unknowable,
-- so keep the span-derived over-estimate: the task fails and retries,
-- the desired outcome for an interval with no data at all.
gate AS
(
    SELECT
        (
            SELECT countIf(sc.committees < e.expected_committees)
            FROM slot_committees AS sc
            INNER JOIN expected_per_epoch AS e ON sc.epoch = e.epoch
        ) AS incomplete_slots,
        (
            SELECT
                if(count() = 0,
                   toInt64(intDiv({{ .bounds.end }} - {{ .bounds.start }}, 12) + 1),
                   toInt64(intDiv(
                       {{ .bounds.end }}
                           - ({{ .bounds.start }} + ((toInt64(any(slot_ts)) - {{ .bounds.start }}) % 12)),
                       12) + 1))
            FROM slot_committees
        ) - toInt64((SELECT count() FROM slot_committees)) AS missing_slots
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    committee_index,
    argMax(v_norm, (votes, toUInt64(cityHash64(v_norm)))) AS validators
FROM votes
WHERE (
    SELECT throwIf(
        incomplete_slots > 0 OR missing_slots > 0,
        'int_beacon_committee_head: interval has missing or incomplete slots (ingestion not settled), failing task for retry'
    )
    FROM gate
) = 0
GROUP BY
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    committee_index
ORDER BY slot, committee_index;
