CREATE TABLE fct_peer_count_by_cl_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent (erigon reported as caplin), unknown when the agent was never seen, other for unparseable names' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version from dim_client_release when the commit or version resolved (for commit_untagged the release the commit builds on), else the version the agent reports, empty when it reports none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers' CODEC(ZSTD(1)),
    `dialled_peer_count` UInt32 COMMENT 'Peers our sentries dialled (outbound connections), so publicly reachable' CODEC(ZSTD(1)),
    `quic_connected_peer_count` UInt32 COMMENT 'Peers connected to over QUIC' CODEC(ZSTD(1)),
    `quic_listen_peer_count` UInt32 COMMENT 'Peers advertising a QUIC listen address in identify' CODEC(ZSTD(1)),
    `custody_known_peer_count` UInt32 COMMENT 'Peers whose custody group count is known from a metadata response' CODEC(ZSTD(1)),
    `custody_4_peer_count` UInt32 COMMENT 'Peers custodying 4 or fewer groups (the minimum)' CODEC(ZSTD(1)),
    `custody_5_8_peer_count` UInt32 COMMENT 'Peers custodying 5 to 8 groups' CODEC(ZSTD(1)),
    `custody_9_127_peer_count` UInt32 COMMENT 'Peers custodying 9 to 127 groups' CODEC(ZSTD(1)),
    `custody_128_peer_count` UInt32 COMMENT 'Peers custodying all 128 groups (supernodes)' CODEC(ZSTD(1)),
    `latency_p50_ms` Nullable(Float64) COMMENT 'Median over peers of each peer median heartbeat latency in ms, NULL without heartbeats' CODEC(ZSTD(1)),
    `latency_p90_ms` Nullable(Float64) COMMENT '90th percentile over peers of each peer median heartbeat latency in ms' CODEC(ZSTD(1)),
    `connection_age_p50_ms` Nullable(Float64) COMMENT 'Median over peers of each peer longest connection age in ms' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `version`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peer census by client and release. Consensus peers our libp2p sentries saw in the day (heartbeats and identify exchanges), one count per peer with its latest agent and address. Crawlers, probes, light clients and our own sentries are excluded. Custody group counts come from metadata responses to our outbound requests. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

CREATE TABLE fct_peer_count_by_cl_client_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_cl_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_cl_client_daily_local,
    cityHash64(`day_start_date`, `client`, `version`, `release_status`)
);

CREATE TABLE fct_peer_count_by_cl_client_country_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent (erigon reported as caplin), unknown when the agent was never seen, other for unparseable names' CODEC(ZSTD(1)),
    `continent_code` LowCardinality(String) COMMENT 'Continent code of the address, other for buckets under 3 peers' CODEC(ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code of the address, other when the country has under 3 peers, unknown when not geolocated' CODEC(ZSTD(1)),
    `country` LowCardinality(String) COMMENT 'Country name, Other or Unknown' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `continent_code`, `country_code`, `country`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peer census by client and country. Consensus peers our libp2p sentries saw in the day (heartbeats and identify exchanges), one count per peer with its latest agent and address. Crawlers, probes, light clients and our own sentries are excluded. Countries under 3 peers roll into their continent other bucket, and continents still under 3 into a global other bucket.';

CREATE TABLE fct_peer_count_by_cl_client_country_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_cl_client_country_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_cl_client_country_daily_local,
    cityHash64(`day_start_date`, `client`, `continent_code`, `country_code`, `country`)
);

CREATE TABLE fct_peer_count_by_cl_client_asn_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent (erigon reported as caplin), unknown when the agent was never seen, other for unparseable names' CODEC(ZSTD(1)),
    `autonomous_system_number` UInt32 COMMENT 'Autonomous system number of the address, 0 for other and unknown' CODEC(ZSTD(1)),
    `autonomous_system_organization` LowCardinality(String) COMMENT 'Autonomous system organisation, other when the AS has under 3 peers, unknown when not geolocated' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs), unknown when not geolocated' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `network_type`, `autonomous_system_organization`, `autonomous_system_number`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peer census by client and autonomous system. Consensus peers our libp2p sentries saw in the day (heartbeats and identify exchanges), one count per peer with its latest agent and address. Crawlers, probes, light clients and our own sentries are excluded. Autonomous systems under 3 peers roll into an other bucket per network type.';

CREATE TABLE fct_peer_count_by_cl_client_asn_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_cl_client_asn_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_cl_client_asn_daily_local,
    cityHash64(`day_start_date`, `client`, `network_type`, `autonomous_system_organization`, `autonomous_system_number`)
);

CREATE TABLE fct_peer_protocol_by_cl_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent (erigon reported as caplin), unknown when the agent was never seen, other for unparseable names' CODEC(ZSTD(1)),
    `protocol` LowCardinality(String) COMMENT 'libp2p protocol ID, e.g. /meshsub/1.3.0 or /eth2/beacon_chain/req/status/2/ssz_snappy' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers supporting the protocol' CODEC(ZSTD(1)),
    `client_peer_count` UInt32 COMMENT 'All peers of the client in the day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `protocol`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily libp2p protocol support by consensus client. Consensus peers our libp2p sentries saw in the day (heartbeats and identify exchanges), one count per peer with its latest agent and address. Crawlers, probes, light clients and our own sentries are excluded. Protocols fewer than 3 of the client peers support are excluded.';

CREATE TABLE fct_peer_protocol_by_cl_client_daily ON CLUSTER '{cluster}' AS fct_peer_protocol_by_cl_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_protocol_by_cl_client_daily_local,
    cityHash64(`day_start_date`, `client`, `protocol`)
);

CREATE TABLE fct_peer_count_by_el_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Execution client from the node devp2p name (nimbus reported as nimbusel)' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version from dim_client_release when the commit or version resolved (for commit_untagged the release the commit builds on), else the version the agent reports, empty when it reports none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Nodes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `version`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily execution node census by client and release. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

CREATE TABLE fct_peer_count_by_el_client_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_el_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_el_client_daily_local,
    cityHash64(`day_start_date`, `client`, `version`, `release_status`)
);

CREATE TABLE fct_peer_count_by_el_client_country_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Execution client from the node devp2p name (nimbus reported as nimbusel)' CODEC(ZSTD(1)),
    `continent_code` LowCardinality(String) COMMENT 'Continent code of the address, other for buckets under 3 peers' CODEC(ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code of the address, other when the country has under 3 peers, unknown when not geolocated' CODEC(ZSTD(1)),
    `country` LowCardinality(String) COMMENT 'Country name, Other or Unknown' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Nodes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `continent_code`, `country_code`, `country`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily execution node census by client and country. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). Countries under 3 nodes roll into their continent other bucket, and continents still under 3 into a global other bucket.';

CREATE TABLE fct_peer_count_by_el_client_country_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_el_client_country_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_el_client_country_daily_local,
    cityHash64(`day_start_date`, `client`, `continent_code`, `country_code`, `country`)
);

CREATE TABLE fct_peer_count_by_el_client_asn_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Execution client from the node devp2p name (nimbus reported as nimbusel)' CODEC(ZSTD(1)),
    `autonomous_system_number` UInt32 COMMENT 'Autonomous system number of the address, 0 for other and unknown' CODEC(ZSTD(1)),
    `autonomous_system_organization` LowCardinality(String) COMMENT 'Autonomous system organisation, other when the AS has under 3 peers, unknown when not geolocated' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs), unknown when not geolocated' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Nodes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `network_type`, `autonomous_system_organization`, `autonomous_system_number`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily execution node census by client and autonomous system. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). Autonomous systems under 3 nodes roll into an other bucket per network type.';

CREATE TABLE fct_peer_count_by_el_client_asn_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_el_client_asn_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_el_client_asn_daily_local,
    cityHash64(`day_start_date`, `client`, `network_type`, `autonomous_system_organization`, `autonomous_system_number`)
);

CREATE TABLE fct_peer_count_by_client_pair_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `consensus_client` LowCardinality(String) COMMENT 'Consensus client at the address' CODEC(ZSTD(1)),
    `execution_client` LowCardinality(String) COMMENT 'Execution client at the address' CODEC(ZSTD(1)),
    `ip_count` UInt32 COMMENT 'Addresses with exactly this consensus and execution client' CODEC(ZSTD(1)),
    `cl_ip_count` UInt32 COMMENT 'Addresses in the day with a consensus client' CODEC(ZSTD(1)),
    `el_ip_count` UInt32 COMMENT 'Addresses in the day with an execution client' CODEC(ZSTD(1)),
    `matched_ip_count` UInt32 COMMENT 'Addresses in the day with both, the match rate is matched_ip_count / cl_ip_count' CODEC(ZSTD(1)),
    `paired_ip_count` UInt32 COMMENT 'Matched addresses with exactly one client per layer, the sum of ip_count over the day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `consensus_client`, `execution_client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus and execution client pairs from nodes sharing an IPv4 address: consensus peers from libp2p and ENRs, execution nodes from discovery. Only addresses with exactly one consensus and one execution client form a pair. No address is stored.';

CREATE TABLE fct_peer_count_by_client_pair_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_client_pair_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_client_pair_daily_local,
    cityHash64(`day_start_date`, `consensus_client`, `execution_client`)
);

CREATE TABLE fct_peer_fork_readiness_by_cl_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the ENR client name (erigon reported as caplin)' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version from dim_client_release when the commit or version resolved (for commit_untagged the release the commit builds on), else the version the agent reports, empty when it reports none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none' CODEC(ZSTD(1)),
    `fork_digest` LowCardinality(String) COMMENT 'Current fork digest in the ENR' CODEC(ZSTD(1)),
    `next_fork_digest` LowCardinality(String) COMMENT 'Next fork digest in the ENR, empty when none' CODEC(ZSTD(1)),
    `node_count` UInt32 COMMENT 'Nodes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `version`, `release_status`, `fork_digest`, `next_fork_digest`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus nodes from discovery (ENRs) by client, release, fork digest and next fork digest. A next fork digest of 0x00000000 or equal to the current digest means no next fork and is stored as empty. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

CREATE TABLE fct_peer_fork_readiness_by_cl_client_daily ON CLUSTER '{cluster}' AS fct_peer_fork_readiness_by_cl_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_fork_readiness_by_cl_client_daily_local,
    cityHash64(`day_start_date`, `client`, `version`, `release_status`, `fork_digest`, `next_fork_digest`)
);

CREATE TABLE fct_peer_fork_readiness_by_el_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Execution client from the node devp2p name (nimbus reported as nimbusel)' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version from dim_client_release when the commit or version resolved (for commit_untagged the release the commit builds on), else the version the agent reports, empty when it reports none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none' CODEC(ZSTD(1)),
    `fork_id_hash` LowCardinality(String) COMMENT 'Fork id hash, the CRC32 of the genesis and passed forks' CODEC(ZSTD(1)),
    `fork_id_next` LowCardinality(String) COMMENT 'Next fork block number or timestamp, empty when none' CODEC(ZSTD(1)),
    `node_count` UInt32 COMMENT 'Nodes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `version`, `release_status`, `fork_id_hash`, `fork_id_next`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily execution nodes from discovery by client, release and EIP-2124 fork id. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). A next fork of 0 means none and is stored as empty. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

CREATE TABLE fct_peer_fork_readiness_by_el_client_daily ON CLUSTER '{cluster}' AS fct_peer_fork_readiness_by_el_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_fork_readiness_by_el_client_daily_local,
    cityHash64(`day_start_date`, `client`, `version`, `release_status`, `fork_id_hash`, `fork_id_next`)
);

CREATE TABLE fct_custody_probe_by_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent (erigon reported as caplin), unknown when the agent was never seen, other for unparseable names' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Release version from dim_client_release when the commit or version resolved (for commit_untagged the release the commit builds on), else the version the agent reports, empty when it reports none' CODEC(ZSTD(1)),
    `release_status` LowCardinality(String) COMMENT 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Probed peers' CODEC(ZSTD(1)),
    `probe_count` UInt32 COMMENT 'Probes' CODEC(ZSTD(1)),
    `success_count` UInt32 COMMENT 'Probes returning the columns' CODEC(ZSTD(1)),
    `missing_count` UInt32 COMMENT 'Probes where the peer did not return a column it should custody' CODEC(ZSTD(1)),
    `failure_count` UInt32 COMMENT 'Probes that failed' CODEC(ZSTD(1)),
    `missing_peer_count` UInt32 COMMENT 'Peers with at least one missing probe' CODEC(ZSTD(1)),
    `response_time_p50_ms` Nullable(Float64) COMMENT 'Median response time of successful probes in ms' CODEC(ZSTD(1)),
    `response_time_p90_ms` Nullable(Float64) COMMENT '90th percentile response time of successful probes in ms' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`, `version`, `release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily data column custody probe outcomes by the probed peer client and release (its latest agent in the day), from int_custody_probe. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

CREATE TABLE fct_custody_probe_by_client_daily ON CLUSTER '{cluster}' AS fct_custody_probe_by_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_custody_probe_by_client_daily_local,
    cityHash64(`day_start_date`, `client`, `version`, `release_status`)
);
