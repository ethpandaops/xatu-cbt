CREATE TABLE `${NETWORK_NAME}`.int_peer_custody_count_by_epoch_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt64 COMMENT 'Epoch number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Start date time of the epoch' CODEC(DoubleDelta, ZSTD(1)),
    `custody_group_count` UInt8 COMMENT 'Number of custody groups the peer is custodying (1-128)' CODEC(ZSTD(1)),
    `peer_id_unique_key` Int64 COMMENT 'Unique key associated with the identifier of the peer' CODEC(ZSTD(1)),
    -- Peer agent metadata
    `peer_agent_implementation` LowCardinality(String) COMMENT 'Implementation of the peer (e.g., Lighthouse, Prysm)' CODEC(ZSTD(1)),
    `peer_agent_version` LowCardinality(String) COMMENT 'Full version string of the peer' CODEC(ZSTD(1)),
    `peer_agent_version_major` LowCardinality(String) COMMENT 'Major version of the peer' CODEC(ZSTD(1)),
    `peer_agent_version_minor` LowCardinality(String) COMMENT 'Minor version of the peer' CODEC(ZSTD(1)),
    `peer_agent_version_patch` LowCardinality(String) COMMENT 'Patch version of the peer' CODEC(ZSTD(1)),
    `peer_agent_platform` LowCardinality(String) COMMENT 'Platform of the peer (e.g., linux-x86_64)' CODEC(ZSTD(1)),
    -- Peer network metadata
    `peer_ip` Nullable(IPv6) COMMENT 'IP address of the peer' CODEC(ZSTD(1)),
    `peer_port` Nullable(UInt16) COMMENT 'Port of the peer' CODEC(ZSTD(1)),
    -- Peer geolocation metadata
    `peer_geo_city` LowCardinality(String) COMMENT 'City of the peer' CODEC(ZSTD(1)),
    `peer_geo_country` LowCardinality(String) COMMENT 'Country of the peer' CODEC(ZSTD(1)),
    `peer_geo_country_code` LowCardinality(String) COMMENT 'Country code of the peer' CODEC(ZSTD(1)),
    `peer_geo_continent_code` LowCardinality(String) COMMENT 'Continent code of the peer' CODEC(ZSTD(1)),
    `peer_geo_longitude` Nullable(Float64) COMMENT 'Longitude of the peer' CODEC(ZSTD(1)),
    `peer_geo_latitude` Nullable(Float64) COMMENT 'Latitude of the peer' CODEC(ZSTD(1)),
    -- Peer ISP/ASN metadata
    `peer_geo_autonomous_system_number` Nullable(UInt32) COMMENT 'Autonomous system number (ASN) of the peer' CODEC(ZSTD(1)),
    `peer_geo_autonomous_system_organization` Nullable(String) COMMENT 'Autonomous system organization (ISP) of the peer' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY toYYYYMM(epoch_start_date_time)
ORDER BY (
    epoch,
    custody_group_count,
    peer_id_unique_key
)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Peer custody count per epoch with full peer metadata for dimension analysis';

CREATE TABLE `${NETWORK_NAME}`.int_peer_custody_count_by_epoch ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_peer_custody_count_by_epoch_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_peer_custody_count_by_epoch_local,
    cityHash64(
        epoch,
        custody_group_count,
        peer_id_unique_key
    )
);
