CREATE TABLE `${NETWORK_NAME}`.int_xatu_nodes__active_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `last_seen_date_time` DateTime COMMENT 'Timestamp when the node was last seen' CODEC(DoubleDelta, ZSTD(1)),
    `username` String COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `is_public` Boolean COMMENT 'Public nodes are not run by ethPandaOps' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the client',
    `meta_client_version` LowCardinality(String) COMMENT 'Version of the client',
    `meta_client_implementation` LowCardinality(String) COMMENT 'Implementation of the client',
    `meta_client_geo_city` LowCardinality(String) COMMENT 'City of the client' CODEC(ZSTD(1)),
    `meta_client_geo_country` LowCardinality(String) COMMENT 'Country of the client' CODEC(ZSTD(1)),
    `meta_client_geo_country_code` LowCardinality(String) COMMENT 'Country code of the client' CODEC(ZSTD(1)),
    `meta_client_geo_continent_code` LowCardinality(String) COMMENT 'Continent code of the client' CODEC(ZSTD(1)),
    `meta_consensus_version` LowCardinality(String) COMMENT 'Ethereum consensus client version',
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Ethereum consensus client implementation'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) ORDER BY
    (`meta_client_name`) COMMENT 'Active nodes for the network';

CREATE TABLE `${NETWORK_NAME}`.int_xatu_nodes__active ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_xatu_nodes__active_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_xatu_nodes__active_local,
    cityHash64(`meta_client_name`)
);
