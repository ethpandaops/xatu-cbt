CREATE TABLE fct_data_column_completion_by_path_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by block slot' CODEC(DoubleDelta, ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Consensus client of the sentry nodes that completed the columns' CODEC(ZSTD(1)),
    `completed_via` LowCardinality(String) COMMENT 'How the columns were completed: FULL_GOSSIP, PARTIAL_GOSSIP, EL, RECONSTRUCTION or UNKNOWN' CODEC(ZSTD(1)),
    `node_count` UInt32 COMMENT 'Distinct sentry nodes with a completion on this path' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Distinct blocks with a column completed on this path' CODEC(ZSTD(1)),
    `completion_count` UInt32 COMMENT 'Column completions, one per node per column' CODEC(ZSTD(1)),
    `cells_total` UInt64 COMMENT 'Cells in the completed columns' CODEC(ZSTD(1)),
    `cells_from_gossip` UInt64 COMMENT 'Cells received over gossip' CODEC(ZSTD(1)),
    `cells_from_el` UInt64 COMMENT 'Cells built from blobs returned by the execution layer' CODEC(ZSTD(1)),
    `avg_contributing_peers` Float64 COMMENT 'Average number of distinct peers that contributed cells to a completion' CODEC(ZSTD(1)),
    `min_completion_slot_start_diff` UInt32 COMMENT 'Earliest completion, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_completion_slot_start_diff` UInt32 COMMENT 'Median completion time, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_completion_slot_start_diff` UInt32 COMMENT '90th percentile completion time, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p99_completion_slot_start_diff` UInt32 COMMENT '99th percentile completion time, in milliseconds from slot start' CODEC(ZSTD(1)),
    `max_completion_slot_start_diff` UInt32 COMMENT 'Latest completion, in milliseconds from slot start' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `meta_consensus_implementation`, `completed_via`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly data column completions on sentry nodes by consensus client and completion path, with cells by source and completion time percentiles';

CREATE TABLE fct_data_column_completion_by_path_hourly ON CLUSTER '{cluster}' AS fct_data_column_completion_by_path_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_data_column_completion_by_path_hourly_local,
    cityHash64(`hour_start_date_time`, `meta_consensus_implementation`, `completed_via`)
);

CREATE TABLE fct_data_column_completion_by_path_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by block slot' CODEC(DoubleDelta, ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Consensus client of the sentry nodes that completed the columns' CODEC(ZSTD(1)),
    `completed_via` LowCardinality(String) COMMENT 'How the columns were completed: FULL_GOSSIP, PARTIAL_GOSSIP, EL, RECONSTRUCTION or UNKNOWN' CODEC(ZSTD(1)),
    `node_count` UInt32 COMMENT 'Distinct sentry nodes with a completion on this path' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Distinct blocks with a column completed on this path' CODEC(ZSTD(1)),
    `completion_count` UInt32 COMMENT 'Column completions, one per node per column' CODEC(ZSTD(1)),
    `cells_total` UInt64 COMMENT 'Cells in the completed columns' CODEC(ZSTD(1)),
    `cells_from_gossip` UInt64 COMMENT 'Cells received over gossip' CODEC(ZSTD(1)),
    `cells_from_el` UInt64 COMMENT 'Cells built from blobs returned by the execution layer' CODEC(ZSTD(1)),
    `avg_contributing_peers` Float64 COMMENT 'Average number of distinct peers that contributed cells to a completion' CODEC(ZSTD(1)),
    `min_completion_slot_start_diff` UInt32 COMMENT 'Earliest completion, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_completion_slot_start_diff` UInt32 COMMENT 'Median completion time, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_completion_slot_start_diff` UInt32 COMMENT '90th percentile completion time, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p99_completion_slot_start_diff` UInt32 COMMENT '99th percentile completion time, in milliseconds from slot start' CODEC(ZSTD(1)),
    `max_completion_slot_start_diff` UInt32 COMMENT 'Latest completion, in milliseconds from slot start' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `meta_consensus_implementation`, `completed_via`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily data column completions on sentry nodes by consensus client and completion path, with cells by source and completion time percentiles';

CREATE TABLE fct_data_column_completion_by_path_daily ON CLUSTER '{cluster}' AS fct_data_column_completion_by_path_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_data_column_completion_by_path_daily_local,
    cityHash64(`day_start_date`, `meta_consensus_implementation`, `completed_via`)
);

CREATE TABLE fct_data_column_partial_message_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by wall clock slot' CODEC(DoubleDelta, ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Consensus client of the sentry nodes. Prysm sentries capture every partial RPC on the wire, Lighthouse sentries only those the gossipsub router passed on' CODEC(ZSTD(1)),
    `node_count` UInt32 COMMENT 'Distinct sentry nodes that sent or received a partial message' CODEC(ZSTD(1)),
    `received_metadata_count` UInt32 COMMENT 'Received partial messages that only carried parts metadata' CODEC(ZSTD(1)),
    `received_message_count` UInt32 COMMENT 'Received partial messages with a body, header-only pushes included' CODEC(ZSTD(1)),
    `received_cell_message_count` UInt32 COMMENT 'Received partial messages that carried at least one cell' CODEC(ZSTD(1)),
    `received_cells` UInt64 COMMENT 'Cells carried by received partial messages, duplicates included' CODEC(ZSTD(1)),
    `received_header_count` UInt32 COMMENT 'Received partial messages that carried the partial data column header' CODEC(ZSTD(1)),
    `cell_sender_peer_count` UInt32 COMMENT 'Distinct peers that sent a sentry node at least one cell' CODEC(ZSTD(1)),
    `sent_metadata_count` UInt32 COMMENT 'Sent partial messages that only carried parts metadata' CODEC(ZSTD(1)),
    `sent_message_count` UInt32 COMMENT 'Sent partial messages with a body, header-only pushes included' CODEC(ZSTD(1)),
    `sent_cell_message_count` UInt32 COMMENT 'Sent partial messages that carried at least one cell' CODEC(ZSTD(1)),
    `sent_cells` UInt64 COMMENT 'Cells carried by sent partial messages' CODEC(ZSTD(1)),
    `sent_header_count` UInt32 COMMENT 'Sent partial messages that carried the partial data column header' CODEC(ZSTD(1)),
    `cells_accept_count` UInt32 COMMENT 'Cell verdicts that accepted a received partial message' CODEC(ZSTD(1)),
    `cells_ignore_count` UInt32 COMMENT 'Cell verdicts that ignored a received partial message, for example because its cells were already known' CODEC(ZSTD(1)),
    `cells_reject_count` UInt32 COMMENT 'Cell verdicts that rejected a received partial message' CODEC(ZSTD(1)),
    `verdict_cells` UInt64 COMMENT 'Cells carried by the partial messages that got a cell verdict' CODEC(ZSTD(1)),
    `new_cells` UInt64 COMMENT 'Cells new to the receiving node that passed verification. Divided by verdict_cells this is the useful cell ratio' CODEC(ZSTD(1)),
    `header_accept_count` UInt32 COMMENT 'Header verdicts that accepted a partial data column header' CODEC(ZSTD(1)),
    `header_ignore_count` UInt32 COMMENT 'Header verdicts that ignored a partial data column header' CODEC(ZSTD(1)),
    `header_reject_count` UInt32 COMMENT 'Header verdicts that rejected a partial data column header' CODEC(ZSTD(1)),
    `header_node_block_count` UInt32 COMMENT 'Node and Fulu block pairs where the node received a partial data column header' CODEC(ZSTD(1)),
    `p50_header_seen_slot_start_diff` UInt32 COMMENT 'Median time a node first received the header for a block, in milliseconds from slot start, 0 when there was none' CODEC(ZSTD(1)),
    `p90_header_seen_slot_start_diff` UInt32 COMMENT '90th percentile time a node first received the header for a block, in milliseconds from slot start, 0 when there was none' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `meta_consensus_implementation`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly gossipsub partial data column traffic on sentry nodes by consensus client: messages and cells sent and received, validation verdicts, useful cells and header push timing';

CREATE TABLE fct_data_column_partial_message_hourly ON CLUSTER '{cluster}' AS fct_data_column_partial_message_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_data_column_partial_message_hourly_local,
    cityHash64(`hour_start_date_time`, `meta_consensus_implementation`)
);

CREATE TABLE fct_data_column_partial_message_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by wall clock slot' CODEC(DoubleDelta, ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Consensus client of the sentry nodes. Prysm sentries capture every partial RPC on the wire, Lighthouse sentries only those the gossipsub router passed on' CODEC(ZSTD(1)),
    `node_count` UInt32 COMMENT 'Distinct sentry nodes that sent or received a partial message' CODEC(ZSTD(1)),
    `received_metadata_count` UInt32 COMMENT 'Received partial messages that only carried parts metadata' CODEC(ZSTD(1)),
    `received_message_count` UInt32 COMMENT 'Received partial messages with a body, header-only pushes included' CODEC(ZSTD(1)),
    `received_cell_message_count` UInt32 COMMENT 'Received partial messages that carried at least one cell' CODEC(ZSTD(1)),
    `received_cells` UInt64 COMMENT 'Cells carried by received partial messages, duplicates included' CODEC(ZSTD(1)),
    `received_header_count` UInt32 COMMENT 'Received partial messages that carried the partial data column header' CODEC(ZSTD(1)),
    `cell_sender_peer_count` UInt32 COMMENT 'Distinct peers that sent a sentry node at least one cell' CODEC(ZSTD(1)),
    `sent_metadata_count` UInt32 COMMENT 'Sent partial messages that only carried parts metadata' CODEC(ZSTD(1)),
    `sent_message_count` UInt32 COMMENT 'Sent partial messages with a body, header-only pushes included' CODEC(ZSTD(1)),
    `sent_cell_message_count` UInt32 COMMENT 'Sent partial messages that carried at least one cell' CODEC(ZSTD(1)),
    `sent_cells` UInt64 COMMENT 'Cells carried by sent partial messages' CODEC(ZSTD(1)),
    `sent_header_count` UInt32 COMMENT 'Sent partial messages that carried the partial data column header' CODEC(ZSTD(1)),
    `cells_accept_count` UInt32 COMMENT 'Cell verdicts that accepted a received partial message' CODEC(ZSTD(1)),
    `cells_ignore_count` UInt32 COMMENT 'Cell verdicts that ignored a received partial message, for example because its cells were already known' CODEC(ZSTD(1)),
    `cells_reject_count` UInt32 COMMENT 'Cell verdicts that rejected a received partial message' CODEC(ZSTD(1)),
    `verdict_cells` UInt64 COMMENT 'Cells carried by the partial messages that got a cell verdict' CODEC(ZSTD(1)),
    `new_cells` UInt64 COMMENT 'Cells new to the receiving node that passed verification. Divided by verdict_cells this is the useful cell ratio' CODEC(ZSTD(1)),
    `header_accept_count` UInt32 COMMENT 'Header verdicts that accepted a partial data column header' CODEC(ZSTD(1)),
    `header_ignore_count` UInt32 COMMENT 'Header verdicts that ignored a partial data column header' CODEC(ZSTD(1)),
    `header_reject_count` UInt32 COMMENT 'Header verdicts that rejected a partial data column header' CODEC(ZSTD(1)),
    `header_node_block_count` UInt32 COMMENT 'Node and Fulu block pairs where the node received a partial data column header' CODEC(ZSTD(1)),
    `p50_header_seen_slot_start_diff` UInt32 COMMENT 'Median time a node first received the header for a block, in milliseconds from slot start, 0 when there was none' CODEC(ZSTD(1)),
    `p90_header_seen_slot_start_diff` UInt32 COMMENT '90th percentile time a node first received the header for a block, in milliseconds from slot start, 0 when there was none' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `meta_consensus_implementation`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily gossipsub partial data column traffic on sentry nodes by consensus client: messages and cells sent and received, validation verdicts, useful cells and header push timing';

CREATE TABLE fct_data_column_partial_message_daily ON CLUSTER '{cluster}' AS fct_data_column_partial_message_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_data_column_partial_message_daily_local,
    cityHash64(`day_start_date`, `meta_consensus_implementation`)
);

CREATE TABLE fct_peer_partial_message_support_by_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `peer_implementation` LowCardinality(String) COMMENT 'Client implementation of the peer from its libp2p agent, empty when no heartbeat identified it' CODEC(ZSTD(1)),
    `peer_version` LowCardinality(String) COMMENT 'Client version of the peer as major.minor.patch, or the raw agent version when it does not parse' CODEC(ZSTD(1)),
    `hour_count` UInt32 COMMENT 'Hours of the day with peer data on the network, the divisor of the averages' CODEC(ZSTD(1)),
    `avg_connected_peer_count` Float64 COMMENT 'Hourly distinct connected peers, averaged over the hours of the day' CODEC(ZSTD(1)),
    `avg_gossipsub_v13_peer_count` Float64 COMMENT 'Hourly connected peers that support gossipsub v1.3, averaged over the hours of the day' CODEC(ZSTD(1)),
    `avg_subscribed_peer_count` Float64 COMMENT 'Hourly peers seen subscribing to a data column topic by a sentry node that reports the partial flags, averaged over the hours of the day' CODEC(ZSTD(1)),
    `avg_requests_partial_peer_count` Float64 COMMENT 'Hourly subscribed peers that requested partial messages, averaged over the hours of the day' CODEC(ZSTD(1)),
    `avg_supports_sending_partial_peer_count` Float64 COMMENT 'Hourly subscribed peers that support sending partial messages, averaged over the hours of the day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `peer_implementation`, `peer_version`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily gossipsub partial messages adoption by peer client and version, as averages of the hourly peer counts';

CREATE TABLE fct_peer_partial_message_support_by_client_daily ON CLUSTER '{cluster}' AS fct_peer_partial_message_support_by_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_partial_message_support_by_client_daily_local,
    cityHash64(`day_start_date`, `peer_implementation`, `peer_version`)
);

CREATE TABLE fct_block_data_column_completion_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'Number of blobs in the block, the number of cells in each column' CODEC(ZSTD(1)),
    `column_count` UInt32 COMMENT 'Distinct columns of the block that a sentry node completed' CODEC(ZSTD(1)),
    `node_count` UInt32 COMMENT 'Distinct sentry nodes that completed a column of the block' CODEC(ZSTD(1)),
    `completion_count` UInt32 COMMENT 'Column completions, one per node per column' CODEC(ZSTD(1)),
    `full_gossip_completion_count` UInt32 COMMENT 'Completions by a full data column gossip message' CODEC(ZSTD(1)),
    `partial_gossip_completion_count` UInt32 COMMENT 'Completions from cells received as gossipsub partial messages' CODEC(ZSTD(1)),
    `el_completion_count` UInt32 COMMENT 'Completions built from blobs returned by the execution layer' CODEC(ZSTD(1)),
    `reconstruction_completion_count` UInt32 COMMENT 'Completions recovered by reconstruction from other columns' CODEC(ZSTD(1)),
    `unknown_completion_count` UInt32 COMMENT 'Completions by a path the node did not report' CODEC(ZSTD(1)),
    `cells_total` UInt64 COMMENT 'Cells in the completed columns, summed over completions' CODEC(ZSTD(1)),
    `cells_from_gossip` UInt64 COMMENT 'Cells received over gossip, summed over completions' CODEC(ZSTD(1)),
    `cells_from_el` UInt64 COMMENT 'Cells built from blobs returned by the execution layer, summed over completions' CODEC(ZSTD(1)),
    `min_completion_slot_start_diff` UInt32 COMMENT 'Earliest completion of any column by any node, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_completion_slot_start_diff` UInt32 COMMENT 'Median completion time across completions, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_completion_slot_start_diff` UInt32 COMMENT '90th percentile completion time across completions, in milliseconds from slot start' CODEC(ZSTD(1)),
    `max_completion_slot_start_diff` UInt32 COMMENT 'Latest completion of any column by any node, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_node_last_completion_slot_start_diff` UInt32 COMMENT 'Median across nodes of the time each node completed its last column of the block, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_node_last_completion_slot_start_diff` UInt32 COMMENT '90th percentile across nodes of the time each node completed its last column of the block, in milliseconds from slot start' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'How sentry nodes completed the data columns of each block: completions by path, cells from gossip and the execution layer, and completion times';

CREATE TABLE fct_block_data_column_completion ON CLUSTER '{cluster}' AS fct_block_data_column_completion_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_data_column_completion_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE fct_block_data_column_completion_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);
