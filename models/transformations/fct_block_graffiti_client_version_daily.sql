---
table: fct_block_graffiti_client_version_daily
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
  - release
dependencies:
  - "{{transformation}}.int_block_graffiti_release"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days
-- that the recount no longer produces (a reprocess moved their blocks to another key) are rewritten as zero.
-- Each block counts once for its consensus client and once for its execution client, keyed by the graffiti commit
-- prefix too, so builds of one version from different commits stay apart.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    period_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_period,
            toDate(max(slot_start_date_time)) AS max_period
        FROM {{ index .dep "{{transformation}}" "int_block_graffiti_release" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    layer,
    client,
    version,
    commit_prefix,
    release_status,
    toUInt32(sum(blocks)) AS block_count
FROM (
    SELECT
        toDate(slot_start_date_time) AS day_start_date,
        layer_client.1 AS layer,
        layer_client.2 AS client,
        layer_client.3 AS version,
        layer_client.4 AS commit_prefix,
        layer_client.5 AS release_status,
        count() AS blocks
    FROM {{ index .dep "{{transformation}}" "int_block_graffiti_release" "helpers" "from" }} FINAL
    ARRAY JOIN [
        ('consensus', consensus_client, consensus_client_version, consensus_client_commit, consensus_client_release_status),
        ('execution', execution_client, execution_client_version, execution_client_commit, execution_client_release_status)
    ] AS layer_client
    WHERE slot_start_date_time >= (SELECT min_period FROM period_bounds)
      AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 DAY
    GROUP BY day_start_date, layer, client, version, commit_prefix, release_status

    UNION ALL

    SELECT
        day_start_date,
        layer,
        client,
        version,
        commit_prefix,
        release_status,
        toUInt64(0) AS blocks
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_period FROM period_bounds)
      AND day_start_date <= (SELECT max_period FROM period_bounds)
      AND block_count > 0
)
GROUP BY day_start_date, layer, client, version, commit_prefix, release_status
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
