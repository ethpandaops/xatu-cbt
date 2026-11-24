---
table: fct_node_count_by_cl_client_version_window
type: scheduled
schedule: "@every 1h"
tags:
  - head
  - node
  - client
dependencies:
  - "{{transformation}}.fct_head_first_seen_by_node_daily"
  - "{{transformation}}.dim_xatu_node"
---
-- Distinct Xatu nodes (as dim_xatu_node chains their names) per consensus client and version over the last 30, 90 and 365 complete UTC days and all
-- history. A node counts once per version however many days it ran it, which daily counts can't give when summed.
-- Every version ever seen gets a row in every window, 0 when no node ran it there, so a version that leaves a
-- window is overwritten rather than left at its old count.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
    today() AS end_day,
    -- dim_xatu_node chains the rotating hashed names of one machine into one Xatu node. Names newer than its last
    -- hourly run count as themselves.
    nodes AS (
        SELECT meta_client_name, stable_node_id
        FROM {{ index .dep "{{transformation}}" "dim_xatu_node" "helpers" "from" }} FINAL
    ),
    days AS (
        SELECT
            t.day_start_date AS day_start_date,
            coalesce(nullIf(n.stable_node_id, ''), t.meta_client_name) AS node_id,
            t.meta_consensus_implementation AS meta_consensus_implementation,
            t.meta_consensus_version AS meta_consensus_version
        FROM {{ index .dep "{{transformation}}" "fct_head_first_seen_by_node_daily" "helpers" "from" }} AS t FINAL
        GLOBAL LEFT JOIN nodes AS n ON t.meta_client_name = n.meta_client_name
        WHERE t.day_start_date < end_day
            AND t.meta_consensus_implementation != ''
    ),
    windows AS (
        SELECT * FROM VALUES(
            'window_name String, window_days UInt32',
            ('30d', 30), ('90d', 90), ('1y', 365), ('all', 0)
        )
    ),
    versions AS (
        SELECT DISTINCT meta_consensus_implementation, meta_consensus_version FROM days
    ),
    counts AS (
        SELECT
            w.window_name AS window_name,
            d.meta_consensus_implementation AS meta_consensus_implementation,
            d.meta_consensus_version AS meta_consensus_version,
            uniqExact(d.node_id) AS node_count
        FROM days AS d
        CROSS JOIN windows AS w
        WHERE w.window_days = 0 OR d.day_start_date >= end_day - w.window_days
        GROUP BY window_name, meta_consensus_implementation, meta_consensus_version
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    w.window_name AS window_name,
    if(w.window_days = 0, (SELECT min(day_start_date) FROM days), end_day - w.window_days) AS window_start_date,
    end_day - 1 AS window_end_date,
    v.meta_consensus_implementation AS meta_consensus_implementation,
    v.meta_consensus_version AS meta_consensus_version,
    toUInt32(ifNull(c.node_count, 0)) AS node_count
FROM versions AS v
CROSS JOIN windows AS w
LEFT JOIN counts AS c
    ON c.window_name = w.window_name
    AND c.meta_consensus_implementation = v.meta_consensus_implementation
    AND c.meta_consensus_version = v.meta_consensus_version
SETTINGS join_use_nulls = 1, parallel_distributed_insert_select = 0
