---
table: fct_block_proposal_by_client_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - block
  - graffiti
  - client
  - mev
  - proposer
dependencies:
  - "{{transformation}}.fct_block_graffiti"
  - "{{transformation}}.int_block_mev_canonical"
  - "{{transformation}}.int_block_canonical"
  - "{{transformation}}.fct_block_first_seen_by_node"
  - "{{transformation}}.fct_block_proposer"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and NULL averages.
-- Canonical blocks take their clients from their own graffiti. Orphaned blocks and missed slots have no
-- canonical graffiti, so they take the proposer's most recent identifying graffiti from the previous 365 days.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    blocks AS (
        SELECT
            slot_start_date_time,
            block_root,
            consensus_client,
            execution_client
        FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    mev AS (
        SELECT block_root, value
        FROM {{ index .dep "{{transformation}}" "int_block_mev_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    payload AS (
        SELECT
            toString(block_root) AS block_root,
            execution_payload_gas_used AS gas_used,
            execution_payload_blob_gas_used AS blob_gas_used
        FROM {{ index .dep "{{transformation}}" "int_block_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    -- Median across nodes of when each node first saw the block.
    first_seen AS (
        SELECT
            block_root,
            quantile(0.5)(seen_slot_start_diff) AS seen_p50_ms
        FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
        GROUP BY block_root
    ),
    failed AS (
        SELECT
            slot_start_date_time,
            proposer_validator_index,
            `status`
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND `status` IN ('orphaned', 'missed')
    ),
    identified AS (
        SELECT
            proposer_index,
            slot_start_date_time,
            consensus_client,
            execution_client
        FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds) - INTERVAL 365 DAY
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND proposer_index GLOBAL IN (SELECT proposer_validator_index FROM failed)
    ),
    identified_consensus AS (
        SELECT proposer_index, slot_start_date_time, consensus_client AS client
        FROM identified
        WHERE consensus_client != 'unknown'
    ),
    identified_execution AS (
        SELECT proposer_index, slot_start_date_time, execution_client AS client
        FROM identified
        WHERE execution_client != 'unknown'
    ),
    proposals AS (
        SELECT
            toDate(b.slot_start_date_time) AS day_start_date,
            b.consensus_client AS consensus_client,
            b.execution_client AS execution_client,
            'canonical' AS proposal_status,
            m.block_root IS NOT NULL AS is_mev,
            m.value AS mev_value,
            p.gas_used AS gas_used,
            p.blob_gas_used AS blob_gas_used,
            f.seen_p50_ms AS seen_p50_ms
        FROM blocks AS b
        GLOBAL LEFT JOIN mev AS m ON b.block_root = m.block_root
        GLOBAL LEFT JOIN payload AS p ON b.block_root = p.block_root
        GLOBAL LEFT JOIN first_seen AS f ON b.block_root = f.block_root

        UNION ALL

        SELECT
            toDate(x.slot_start_date_time) AS day_start_date,
            if(cl.slot_start_date_time >= x.slot_start_date_time - INTERVAL 365 DAY, cl.client, 'unknown') AS consensus_client,
            if(el.slot_start_date_time >= x.slot_start_date_time - INTERVAL 365 DAY, el.client, 'unknown') AS execution_client,
            x.`status` AS proposal_status,
            false AS is_mev,
            NULL AS mev_value,
            NULL AS gas_used,
            NULL AS blob_gas_used,
            NULL AS seen_p50_ms
        FROM failed AS x
        ASOF LEFT JOIN identified_consensus AS cl
            ON x.proposer_validator_index = cl.proposer_index AND x.slot_start_date_time > cl.slot_start_date_time
        ASOF LEFT JOIN identified_execution AS el
            ON x.proposer_validator_index = el.proposer_index AND x.slot_start_date_time > el.slot_start_date_time
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    layer,
    client,
    toUInt32(sum(block_count)) AS block_count,
    toUInt32(sum(mev_block_count)) AS mev_block_count,
    max(avg_mev_value_eth) AS avg_mev_value_eth,
    max(avg_gas_used) AS avg_gas_used,
    max(avg_blob_count) AS avg_blob_count,
    toUInt32(sum(blob_count)) AS blob_count,
    max(block_first_seen_p50_ms) AS block_first_seen_p50_ms,
    max(block_first_seen_p90_ms) AS block_first_seen_p90_ms,
    toUInt32(sum(orphaned_block_count)) AS orphaned_block_count,
    toUInt32(sum(missed_slot_count)) AS missed_slot_count
FROM (
    SELECT
        day_start_date,
        layer_client.1 AS layer,
        ifNull(layer_client.2, 'unknown') AS client,
        countIf(proposal_status = 'canonical') AS block_count,
        countIf(is_mev) AS mev_block_count,
        if(isNaN(avgIf(toFloat64(mev_value) / 1e18, is_mev AND mev_value IS NOT NULL)), NULL, avgIf(toFloat64(mev_value) / 1e18, is_mev AND mev_value IS NOT NULL)) AS avg_mev_value_eth,
        if(isNaN(avgIf(toFloat64(gas_used), gas_used IS NOT NULL)), NULL, avgIf(toFloat64(gas_used), gas_used IS NOT NULL)) AS avg_gas_used,
        if(isNaN(avgIf(toFloat64(blob_gas_used) / 131072, blob_gas_used IS NOT NULL)), NULL, avgIf(toFloat64(blob_gas_used) / 131072, blob_gas_used IS NOT NULL)) AS avg_blob_count,
        sumIf(intDiv(blob_gas_used, 131072), blob_gas_used IS NOT NULL) AS blob_count,
        if(isNaN(quantileIf(0.5)(seen_p50_ms, seen_p50_ms IS NOT NULL)), NULL, quantileIf(0.5)(seen_p50_ms, seen_p50_ms IS NOT NULL)) AS block_first_seen_p50_ms,
        if(isNaN(quantileIf(0.9)(seen_p50_ms, seen_p50_ms IS NOT NULL)), NULL, quantileIf(0.9)(seen_p50_ms, seen_p50_ms IS NOT NULL)) AS block_first_seen_p90_ms,
        countIf(proposal_status = 'orphaned') AS orphaned_block_count,
        countIf(proposal_status = 'missed') AS missed_slot_count
    FROM proposals
    ARRAY JOIN [('consensus', consensus_client), ('execution', execution_client)] AS layer_client
    GROUP BY day_start_date, layer, client

    UNION ALL

    SELECT
        day_start_date,
        layer,
        client,
        0 AS block_count,
        0 AS mev_block_count,
        NULL AS avg_mev_value_eth,
        NULL AS avg_gas_used,
        NULL AS avg_blob_count,
        0 AS blob_count,
        NULL AS block_first_seen_p50_ms,
        NULL AS block_first_seen_p90_ms,
        0 AS orphaned_block_count,
        0 AS missed_slot_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, layer, client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
