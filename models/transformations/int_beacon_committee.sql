---
table: int_beacon_committee
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - beacon
  - committee
  - canonical
dependencies:
  - "{{external}}.beacon_api_eth_v1_beacon_committee"
  - "{{external}}.canonical_beacon_committee"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Committee assignment is deterministic per epoch (fixed by the RANDAO seed
-- epochs in advance), so the head-event stream and the canonical table can
-- never disagree on CONTENT, only on presence. This is the settled merge:
-- it AND-depends on canonical, so it lags finalization, and where both
-- sources have a (slot, committee_index) it prefers the canonical validator
-- set. The head-event stream backfills committees whose canonical batch has
-- not landed yet within the dependency's available range.
head AS
(
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        committee_index,
        arraySort(arrayDistinct(validators)) AS v_norm
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_committee" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY
        slot, slot_start_date_time, epoch, epoch_start_date_time, committee_index, validators
),

canonical AS
(
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        committee_index,
        arraySort(arrayDistinct(validators)) AS v_norm
    FROM {{ index .dep "{{external}}" "canonical_beacon_committee" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY
        slot, slot_start_date_time, epoch, epoch_start_date_time, committee_index, validators
),

merged AS
(
    SELECT
        COALESCE(c.slot, h.slot) AS slot,
        COALESCE(c.slot_start_date_time, h.slot_start_date_time) AS slot_start_date_time,
        COALESCE(c.epoch, h.epoch) AS epoch,
        COALESCE(c.epoch_start_date_time, h.epoch_start_date_time) AS epoch_start_date_time,
        COALESCE(c.committee_index, h.committee_index) AS committee_index,
        CASE
            WHEN length(c.v_norm) > 0 THEN c.v_norm
            ELSE h.v_norm
        END AS validators
    FROM canonical c
    GLOBAL FULL OUTER JOIN head h
        ON c.slot = h.slot AND c.committee_index = h.committee_index
),

-- Sentries publish a whole epoch's committees as one batch around the epoch
-- boundary, so the external bound can cover a slot while that batch is still
-- being ingested. Require every slot in the interval to have as many
-- committees as its epoch shows anywhere in a +-2 epoch window, and fail the
-- task otherwise so the scheduler retries once ingestion has settled.
-- Presence is the union of both sources.
presence_window AS
(
    SELECT slot, epoch, committee_index, slot_start_date_time
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_committee" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 768 SECOND
                                   AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 768 SECOND
        AND meta_network_name = '{{ .env.NETWORK }}'
    UNION ALL
    SELECT slot, epoch, committee_index, slot_start_date_time
    FROM {{ index .dep "{{external}}" "canonical_beacon_committee" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 768 SECOND
                                   AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 768 SECOND
        AND meta_network_name = '{{ .env.NETWORK }}'
),

expected_per_epoch AS
(
    SELECT
        epoch,
        uniqExact(committee_index) AS expected_committees
    FROM presence_window
    GROUP BY epoch
),

slot_committees AS
(
    SELECT
        slot,
        any(epoch) AS epoch,
        any(toUnixTimestamp(slot_start_date_time)) AS slot_ts,
        uniqExact(committee_index) AS committees
    FROM presence_window
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot
),

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
    validators
FROM merged
-- The gate protects against ingestion RACES, which settle within seconds. An
-- interval whose end is more than an hour old is settled: anything absent from
-- BOTH the head-event stream and the canonical table by then is permanently
-- absent, and refusing it forever would stall backfill at the first such gap.
WHERE (
    SELECT throwIf(
        (incomplete_slots > 0 OR missing_slots > 0)
            AND fromUnixTimestamp({{ .bounds.end }}) > now() - INTERVAL 1 HOUR,
        'int_beacon_committee: interval has missing or incomplete slots (ingestion not settled), failing task for retry'
    )
    FROM gate
) = 0
ORDER BY slot, committee_index;
