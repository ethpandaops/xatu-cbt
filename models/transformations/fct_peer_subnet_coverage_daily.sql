---
table: fct_peer_subnet_coverage_daily
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 1m"
tags:
  - daily
  - peer
  - libp2p
  - subnet
dependencies:
  - "{{transformation}}.fct_peer_subnet_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Days the recount no longer produces
-- are rewritten with zero counts.
-- One row a day on how evenly consensus peers cover the 64 attestation and 4 sync subnets, from
-- fct_peer_subnet_daily: the thinnest, median and thickest attestation subnet, the thinnest without peers on
-- every subnet, and the thinnest and thickest sync subnet.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    toUInt32(sum(all_n)) AS attnets_64_peer_count,
    toUInt32(sum(att_min_n)) AS min_attestation_subnet_peer_count,
    toUInt8(sum(att_min_id)) AS min_attestation_subnet_id,
    toUInt32(sum(att_median_n)) AS median_attestation_subnet_peer_count,
    toUInt32(sum(att_max_n)) AS max_attestation_subnet_peer_count,
    toUInt32(sum(att_min_partial_n)) AS min_attestation_subnet_partial_peer_count,
    toUInt32(sum(sync_min_n)) AS min_sync_subnet_peer_count,
    toUInt32(sum(sync_max_n)) AS max_sync_subnet_peer_count
FROM (
    SELECT
        day_start_date,
        -- Peers on all 64 subnets are counted on every attestation row.
        maxIf(all_subnets_peer_count, subnet_type = 'attestation') AS all_n,
        minIf(peer_count, subnet_type = 'attestation') AS att_min_n,
        -- The lowest subnet id among the thinnest subnets.
        argMinIf(subnet_id, (peer_count, subnet_id), subnet_type = 'attestation') AS att_min_id,
        quantileExactLowIf(0.5)(peer_count, subnet_type = 'attestation') AS att_median_n,
        maxIf(peer_count, subnet_type = 'attestation') AS att_max_n,
        minIf(peer_count - all_subnets_peer_count, subnet_type = 'attestation') AS att_min_partial_n,
        minIf(peer_count, subnet_type = 'sync') AS sync_min_n,
        maxIf(peer_count, subnet_type = 'sync') AS sync_max_n
    FROM {{ index .dep "{{transformation}}" "fct_peer_subnet_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
    GROUP BY day_start_date
    -- Days whose subnet rows were all zeroed on a recount have no peers left.
    HAVING max(peer_count) > 0

    UNION ALL

    SELECT
        day_start_date,
        0 AS all_n,
        0 AS att_min_n,
        0 AS att_min_id,
        0 AS att_median_n,
        0 AS att_max_n,
        0 AS att_min_partial_n,
        0 AS sync_min_n,
        0 AS sync_max_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
