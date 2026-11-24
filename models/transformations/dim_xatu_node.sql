---
table: dim_xatu_node
type: scheduled
schedule: "@every 1h"
tags:
  - xatu
  - nodes
dependencies:
  - "{{transformation}}.fct_block_first_seen_by_node_hourly"
---
-- contributoor names a node after a hash of its beacon node's p2p ID, so the name changes whenever that key
-- changes (nimbus makes a new key on every restart unless --netkey-file is set). One machine then shows up as many
-- hashed names. Names of one contributor and consensus client are chained into one node when a name starts in or
-- after the hour the previous one was last seen. Names active at the same time are separate nodes. Each name joins
-- the chain that ended earliest among those it fits, so concurrent nodes keep their own chains. Named (non-hashed)
-- nodes do not rotate and map to themselves. The whole table is recomputed each run.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH ids AS (
    SELECT
        meta_client_name,
        argMax(username, hour_start_date_time) AS username,
        argMax(meta_consensus_implementation, hour_start_date_time) AS meta_consensus_implementation,
        min(hour_start_date_time) AS first_seen_date_time,
        max(hour_start_date_time) AS last_seen_date_time
    FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node_hourly" "helpers" "from" }} FINAL
    GROUP BY meta_client_name
),
groups AS (
    SELECT
        username,
        meta_consensus_implementation,
        arraySort(m -> (m.2, m.1), groupArray((meta_client_name, first_seen_date_time, last_seen_date_time))) AS members
    FROM ids
    WHERE position(meta_client_name, '/hashed-') > 0
    GROUP BY username, meta_consensus_implementation
),
-- The fold state holds the open chains as (last seen, chain head) and the assignments as (name, chain head).
chained AS (
    SELECT
        arrayFold(
            (acc, m) -> if(
                empty(arrayFilter(c -> c.1 <= m.2, acc.1)),
                (arrayPushBack(acc.1, (m.3, m.1)), arrayPushBack(acc.2, (m.1, m.1))),
                (
                    arrayMap(c -> if(c = arraySort(arrayFilter(c -> c.1 <= m.2, acc.1))[1], (m.3, c.2), c), acc.1),
                    arrayPushBack(acc.2, (m.1, arraySort(arrayFilter(c -> c.1 <= m.2, acc.1))[1].2))
                )
            ),
            members,
            (CAST([], 'Array(Tuple(DateTime, String))'), CAST([], 'Array(Tuple(String, String))'))
        ).2 AS assignments
    FROM groups
),
heads AS (
    SELECT a.1 AS meta_client_name, a.2 AS stable_node_id
    FROM chained
    ARRAY JOIN assignments AS a
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    i.meta_client_name AS meta_client_name,
    if(h.stable_node_id = '', i.meta_client_name, h.stable_node_id) AS stable_node_id,
    i.username AS username,
    i.meta_consensus_implementation AS meta_consensus_implementation,
    i.first_seen_date_time AS first_seen_date_time,
    i.last_seen_date_time AS last_seen_date_time
FROM ids AS i
LEFT JOIN heads AS h ON i.meta_client_name = h.meta_client_name
