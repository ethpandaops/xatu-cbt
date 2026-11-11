CREATE TABLE int_peer_subnet_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `peer_id_unique_key` Int64 COMMENT 'Hash of the peer ID and network, the key shared by the libp2p tables' CODEC(ZSTD(1)),
    `attnets` UInt64 COMMENT 'Attestation subnets the peer last advertised in the day: bit i is subnet i' CODEC(ZSTD(1)),
    `syncnets` UInt8 COMMENT 'Sync committee subnets the peer last advertised in the day: bit i is subnet i' CODEC(ZSTD(1)),
    `response_count` UInt32 COMMENT 'Metadata responses the peer sent our sentries in the day. 0 means the peer was dropped from the day on a recount' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `peer_id_unique_key`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'One row per consensus peer per day with the attestation and sync subnets it last advertised in a metadata response to our libp2p sentries. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE int_peer_subnet_daily ON CLUSTER '{cluster}' AS int_peer_subnet_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_peer_subnet_daily_local,
    cityHash64(`day_start_date`, `peer_id_unique_key`)
);

CREATE TABLE fct_peer_attnet_count_by_cl_client_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent: lighthouse, lodestar, nimbus, prysm, teku, grandine, caplin (erigon), silver or ethlambda, unknown when the agent was never seen, other for every other agent' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers that answered our sentries metadata requests in the day' CODEC(ZSTD(1)),
    `attnets_0_peer_count` UInt32 COMMENT 'Peers on no attestation subnet' CODEC(ZSTD(1)),
    `attnets_1_2_peer_count` UInt32 COMMENT 'Peers on 1 or 2 attestation subnets (2 is the long-lived default)' CODEC(ZSTD(1)),
    `attnets_3_63_peer_count` UInt32 COMMENT 'Peers on 3 to 63 attestation subnets' CODEC(ZSTD(1)),
    `attnets_64_peer_count` UInt32 COMMENT 'Peers on all 64 attestation subnets' CODEC(ZSTD(1)),
    `syncnets_any_peer_count` UInt32 COMMENT 'Peers on at least one sync committee subnet' CODEC(ZSTD(1)),
    `syncnets_4_peer_count` UInt32 COMMENT 'Peers on all 4 sync committee subnets' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peers by client and by how many attestation and sync subnets they last advertised in a metadata response to our libp2p sentries. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_attnet_count_by_cl_client_daily ON CLUSTER '{cluster}' AS fct_peer_attnet_count_by_cl_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_attnet_count_by_cl_client_daily_local,
    cityHash64(`day_start_date`)
);

CREATE TABLE fct_peer_subnet_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `subnet_type` LowCardinality(String) COMMENT 'attestation (64 subnets) or sync (4 sync committee subnets)' CODEC(ZSTD(1)),
    `subnet_id` UInt8 COMMENT 'Subnet index, 0 to 63 for attestation and 0 to 3 for sync' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers that last advertised the subnet in the day' CODEC(ZSTD(1)),
    `all_subnets_peer_count` UInt32 COMMENT 'Of those, peers on every subnet of the type, the same on every row of the type' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `subnet_type`, `subnet_id`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peers by the attestation and sync subnets they last advertised in a metadata response to our libp2p sentries, 68 rows a day. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_subnet_daily ON CLUSTER '{cluster}' AS fct_peer_subnet_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_subnet_daily_local,
    cityHash64(`day_start_date`)
);

CREATE TABLE fct_peer_subnet_coverage_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `attnets_64_peer_count` UInt32 COMMENT 'Peers on all 64 attestation subnets' CODEC(ZSTD(1)),
    `min_attestation_subnet_peer_count` UInt32 COMMENT 'Peers on the thinnest attestation subnet' CODEC(ZSTD(1)),
    `min_attestation_subnet_id` UInt8 COMMENT 'The thinnest attestation subnet, the lowest index on a tie' CODEC(ZSTD(1)),
    `median_attestation_subnet_peer_count` UInt32 COMMENT 'Median peers per attestation subnet (the lower of the middle two)' CODEC(ZSTD(1)),
    `max_attestation_subnet_peer_count` UInt32 COMMENT 'Peers on the thickest attestation subnet' CODEC(ZSTD(1)),
    `min_attestation_subnet_partial_peer_count` UInt32 COMMENT 'Fewest peers any attestation subnet has without the peers on all 64' CODEC(ZSTD(1)),
    `min_sync_subnet_peer_count` UInt32 COMMENT 'Peers on the thinnest sync committee subnet' CODEC(ZSTD(1)),
    `max_sync_subnet_peer_count` UInt32 COMMENT 'Peers on the thickest sync committee subnet' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily spread of consensus peers over the 64 attestation and 4 sync subnets they advertise, one row a day: the thinnest, median and thickest attestation subnet, the thinnest without peers on all 64, and the thinnest and thickest sync subnet. Peers as in fct_peer_subnet_daily.';

CREATE TABLE fct_peer_subnet_coverage_daily ON CLUSTER '{cluster}' AS fct_peer_subnet_coverage_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_subnet_coverage_daily_local,
    cityHash64(`day_start_date`)
);
