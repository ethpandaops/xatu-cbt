CREATE TABLE fct_data_column_completion_by_slot_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `column_index` UInt64 COMMENT 'The data column index' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'Number of blobs in the block, the number of cells in the column' CODEC(ZSTD(1)),
    `completion_node_count` UInt32 COMMENT 'Number of sentry nodes that reported the column complete' CODEC(ZSTD(1)),
    `full_gossip_completion_count` UInt32 COMMENT 'Number of nodes whose column was completed by a full gossip message' CODEC(ZSTD(1)),
    `partial_gossip_completion_count` UInt32 COMMENT 'Number of nodes whose column was completed from cells received as gossipsub partial messages' CODEC(ZSTD(1)),
    `el_completion_count` UInt32 COMMENT 'Number of nodes whose column was built from blobs returned by the execution layer' CODEC(ZSTD(1)),
    `reconstruction_completion_count` UInt32 COMMENT 'Number of nodes whose column was recovered by reconstruction from other columns' CODEC(ZSTD(1)),
    `unknown_completion_count` UInt32 COMMENT 'Number of nodes whose column was completed by a path the node did not report' CODEC(ZSTD(1)),
    `cells_total` UInt64 COMMENT 'Cells in the completed columns, summed over the nodes that completed it' CODEC(ZSTD(1)),
    `cells_from_gossip` UInt64 COMMENT 'Cells received over gossip, summed over the nodes that completed the column' CODEC(ZSTD(1)),
    `cells_from_el` UInt64 COMMENT 'Cells built from blobs returned by the execution layer, summed over the nodes that completed the column' CODEC(ZSTD(1)),
    `avg_partial_contributing_peers` Float64 COMMENT 'Average number of distinct peers that contributed cells to a PARTIAL_GOSSIP completion, 0 when there was none' CODEC(ZSTD(1)),
    `min_completion_slot_start_diff` UInt32 COMMENT 'Earliest completion of the column by any node, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_completion_slot_start_diff` UInt32 COMMENT 'Median completion time across nodes, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_completion_slot_start_diff` UInt32 COMMENT '90th percentile completion time across nodes, in milliseconds from slot start' CODEC(ZSTD(1)),
    `max_completion_slot_start_diff` UInt32 COMMENT 'Latest completion of the column by any node, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p50_full_gossip_completion_slot_start_diff` UInt32 COMMENT 'Median completion time across FULL_GOSSIP completions, in milliseconds from slot start, 0 when there was none' CODEC(ZSTD(1)),
    `p50_partial_gossip_completion_slot_start_diff` UInt32 COMMENT 'Median completion time across PARTIAL_GOSSIP completions, in milliseconds from slot start, 0 when there was none' CODEC(ZSTD(1)),
    `p50_el_completion_slot_start_diff` UInt32 COMMENT 'Median completion time across EL completions, in milliseconds from slot start, 0 when there was none' CODEC(ZSTD(1)),
    `partial_received_message_count` UInt32 COMMENT 'Partial messages with a body received by sentry nodes, summed over nodes. WIRE capture points count every part, APP capture points only those the gossipsub router passed on' CODEC(ZSTD(1)),
    `partial_received_cells` UInt64 COMMENT 'Cells carried by the partial messages received by sentry nodes, duplicates included' CODEC(ZSTD(1)),
    `partial_sender_peer_count` UInt32 COMMENT 'Distinct peers that sent a partial message with a body for the column to a sentry node' CODEC(ZSTD(1)),
    `partial_sent_message_count` UInt32 COMMENT 'Partial messages with a body sent by sentry nodes, summed over nodes' CODEC(ZSTD(1)),
    `partial_sent_cells` UInt64 COMMENT 'Cells carried by the partial messages sent by sentry nodes' CODEC(ZSTD(1)),
    `partial_accept_verdict_count` UInt32 COMMENT 'Validation verdicts on received partial messages that accepted them' CODEC(ZSTD(1)),
    `partial_ignore_verdict_count` UInt32 COMMENT 'Validation verdicts on received partial messages that ignored them' CODEC(ZSTD(1)),
    `partial_reject_verdict_count` UInt32 COMMENT 'Validation verdicts on received partial messages that rejected them' CODEC(ZSTD(1)),
    `partial_new_cells` UInt64 COMMENT 'Cells new to the receiving node that passed verification. Divided by partial_received_cells this approximates the useful cell ratio' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`, `column_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'How each data column of a block became available across sentry nodes: completions by path, cells from gossip and the execution layer, and gossipsub partial message traffic';

CREATE TABLE fct_data_column_completion_by_slot ON CLUSTER '{cluster}' AS fct_data_column_completion_by_slot_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_data_column_completion_by_slot_local,
    cityHash64(`slot_start_date_time`, `block_root`, `column_index`)
);

ALTER TABLE fct_data_column_completion_by_slot_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`, `column_index`)
);

CREATE TABLE fct_block_partial_data_column_header_first_seen_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `proposer_index` UInt32 COMMENT 'The validator index of the block proposer, from the header' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'Number of KZG commitments in the header' CODEC(ZSTD(1)),
    `seen_slot_start_diff` UInt32 COMMENT 'Time from slot start until any sentry node first received a partial data column header for the block, in milliseconds' CODEC(ZSTD(1)),
    `p50_node_seen_slot_start_diff` UInt32 COMMENT 'Median across nodes of the time each node first received a header for the block, in milliseconds from slot start' CODEC(ZSTD(1)),
    `p90_node_seen_slot_start_diff` UInt32 COMMENT '90th percentile across nodes of the time each node first received a header for the block, in milliseconds from slot start' CODEC(ZSTD(1)),
    `max_node_seen_slot_start_diff` UInt32 COMMENT 'Latest time a node first received a header for the block, in milliseconds from slot start' CODEC(ZSTD(1)),
    `node_count` UInt32 COMMENT 'Number of sentry nodes that received a partial data column header for the block' CODEC(ZSTD(1)),
    `first_seen_meta_client_name` LowCardinality(String) COMMENT 'Name of the sentry node that received a header first' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'When sentry nodes first received the gossipsub partial data column header for a Fulu block, the eager push that lets peers verify cells before the full column arrives';

CREATE TABLE fct_block_partial_data_column_header_first_seen ON CLUSTER '{cluster}' AS fct_block_partial_data_column_header_first_seen_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_partial_data_column_header_first_seen_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE fct_block_partial_data_column_header_first_seen_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);

CREATE TABLE fct_peer_partial_message_support_by_client_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour' CODEC(DoubleDelta, ZSTD(1)),
    `peer_implementation` LowCardinality(String) COMMENT 'Client implementation of the peer from its libp2p agent, empty when no heartbeat identified it' CODEC(ZSTD(1)),
    `peer_version` LowCardinality(String) COMMENT 'Client version of the peer as major.minor.patch, or the raw agent version when it does not parse' CODEC(ZSTD(1)),
    `connected_peer_count` UInt32 COMMENT 'Distinct peers connected to a sentry node during the hour, from heartbeats' CODEC(ZSTD(1)),
    `gossipsub_v13_peer_count` UInt32 COMMENT 'Connected peers that support gossipsub v1.3 (/meshsub/1.3.0), which the partial messages extension requires' CODEC(ZSTD(1)),
    `subscribed_peer_count` UInt32 COMMENT 'Distinct peers seen subscribing to a data column topic during the hour by a sentry node that reports the partial flags' CODEC(ZSTD(1)),
    `requests_partial_peer_count` UInt32 COMMENT 'Subscribed peers that requested partial messages on a data column topic' CODEC(ZSTD(1)),
    `supports_sending_partial_peer_count` UInt32 COMMENT 'Subscribed peers that support sending partial messages on a data column topic' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `peer_implementation`, `peer_version`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly gossipsub partial messages adoption by peer client and version: connected peers, gossipsub v1.3 support and the partial flags peers set when subscribing to data column topics';

CREATE TABLE fct_peer_partial_message_support_by_client_hourly ON CLUSTER '{cluster}' AS fct_peer_partial_message_support_by_client_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_partial_message_support_by_client_hourly_local,
    cityHash64(`hour_start_date_time`, `peer_implementation`, `peer_version`)
);
