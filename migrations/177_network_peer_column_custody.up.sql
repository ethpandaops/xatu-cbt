CREATE TABLE dim_peer_node_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `peer_id_unique_key` Int64 COMMENT 'Hash of the peer ID and network, the key shared by the libp2p tables' CODEC(ZSTD(1)),
    `peer_id` String COMMENT 'libp2p peer ID' CODEC(ZSTD(1)),
    `node_id` String COMMENT 'discv5 node ID (hex, the keccak256 of the uncompressed secp256k1 public key in the peer ID), empty when the peer ID holds another key type' CODEC(ZSTD(1)),
    `custody_group_order` Array(UInt8) COMMENT 'All 128 custody groups in the order the node ID assigns them (get_custody_groups): a peer with custody group count n custodies the first n, and group g is data column g. Empty without a node ID' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY
    (`peer_id_unique_key`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Consensus peers in int_peer_daily with a peer ID, with their discv5 node ID and the order their node ID assigns custody groups in.';

CREATE TABLE dim_peer_node ON CLUSTER '{cluster}' AS dim_peer_node_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    dim_peer_node_local,
    cityHash64(`peer_id_unique_key`)
);

CREATE TABLE fct_peer_custody_by_column_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `column_index` UInt8 COMMENT 'Data column index, 0 to 127' CODEC(ZSTD(1)),
    `custodian_peer_count` UInt32 COMMENT 'Peers that custody the column, from their custody group count and node ID' CODEC(ZSTD(1)),
    `supernode_peer_count` UInt32 COMMENT 'Of those, peers custodying all 128 groups (supernodes), the same for every column' CODEC(ZSTD(1)),
    `hosting_peer_count` UInt32 COMMENT 'Of those, peers on a cloud or hosting provider network' CODEC(ZSTD(1)),
    `country_count` UInt16 COMMENT 'Countries the custodians are in' CODEC(ZSTD(1)),
    `largest_country_peer_count` UInt32 COMMENT 'Custodians in the country with the most of them' CODEC(ZSTD(1)),
    `hosting_autonomous_system_count` UInt16 COMMENT 'Hosting autonomous systems the custodians are on. Residential peers have no autonomous system here' CODEC(ZSTD(1)),
    `largest_autonomous_system_peer_count` UInt32 COMMENT 'Custodians on the hosting autonomous system with the most of them' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `column_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peers by the data columns they custody, 128 rows a day: a peer with custody group count n custodies the first n groups its node ID assigns. Peers our libp2p sentries or discovery crawler saw with a known custody group count and peer ID (all peers from April 2026). Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_custody_by_column_daily ON CLUSTER '{cluster}' AS fct_peer_custody_by_column_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_custody_by_column_daily_local,
    cityHash64(`day_start_date`)
);

CREATE TABLE fct_peer_custody_coverage_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `custody_peer_count` UInt32 COMMENT 'Peers counted: those with a known custody group count and node ID' CODEC(ZSTD(1)),
    `supernode_peer_count` UInt32 COMMENT 'Of those, peers custodying all 128 groups (supernodes)' CODEC(ZSTD(1)),
    `min_column_custodian_peer_count` UInt32 COMMENT 'Custodians of the thinnest column' CODEC(ZSTD(1)),
    `min_column_index` UInt8 COMMENT 'The thinnest column, the lowest index on a tie' CODEC(ZSTD(1)),
    `median_column_custodian_peer_count` UInt32 COMMENT 'Median custodians per column (the lower of the middle two)' CODEC(ZSTD(1)),
    `max_column_custodian_peer_count` UInt32 COMMENT 'Custodians of the thickest column' CODEC(ZSTD(1)),
    `min_column_non_supernode_peer_count` UInt32 COMMENT 'Custodians of the thinnest column without supernodes' CODEC(ZSTD(1)),
    `min_column_without_largest_autonomous_system_peer_count` UInt32 COMMENT 'Fewest custodians any column keeps once its largest hosting autonomous system is taken away' CODEC(ZSTD(1)),
    `min_column_country_count` UInt16 COMMENT 'Fewest countries any column is custodied in' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily spread of consensus peers over the 128 data columns they custody, one row a day: the thinnest, median and thickest column, and the thinnest once supernodes or its largest hosting autonomous system are taken away. Peers as in fct_peer_custody_by_column_daily.';

CREATE TABLE fct_peer_custody_coverage_daily ON CLUSTER '{cluster}' AS fct_peer_custody_coverage_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_custody_coverage_daily_local,
    cityHash64(`day_start_date`)
);
