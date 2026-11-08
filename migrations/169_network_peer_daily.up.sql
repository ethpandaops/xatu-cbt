CREATE TABLE int_peer_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `peer_id_unique_key` Int64 COMMENT 'Hash of the peer ID and network, the key shared by the libp2p tables' CODEC(ZSTD(1)),
    `peer_id` String COMMENT 'libp2p peer ID, empty when the peer was only seen before peer IDs were recorded (April 2026)' CODEC(ZSTD(1)),
    `seen_by_sentry` Bool COMMENT 'Our libp2p sentries were connected to the peer in the day. False on both sources means the peer was dropped from the day on a recount' CODEC(ZSTD(1)),
    `seen_by_enr` Bool COMMENT 'Our discovery crawler dialled the peer from its ENR in the day' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent, else its ENR name (erigon reported as caplin), unknown when neither names one, other for unparseable names' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Version the agent states (semver with any pre-release suffix), empty when it states none' CODEC(ZSTD(1)),
    `agent_version` String COMMENT 'Version field of the agent as reported, for Caplin its platform field' CODEC(ZSTD(1)),
    `continent_code` LowCardinality(String) COMMENT 'Continent code of the address, empty when not geolocated' CODEC(ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code of the address, empty when not geolocated' CODEC(ZSTD(1)),
    `country` LowCardinality(String) COMMENT 'Country name of the address, empty when not geolocated' CODEC(ZSTD(1)),
    `city` LowCardinality(String) COMMENT 'City of the address for hosting peers, empty for residential peers (network_type other or unknown) and when not geolocated to a city' CODEC(ZSTD(1)),
    `autonomous_system_number` UInt32 COMMENT 'Autonomous system number of the address for hosting peers, 0 for residential peers and when unknown' CODEC(ZSTD(1)),
    `autonomous_system_organization` LowCardinality(String) COMMENT 'Autonomous system organisation of the address for hosting peers, empty for residential peers and when unknown' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs), unknown when not geolocated' CODEC(ZSTD(1)),
    `provider` LowCardinality(String) COMMENT 'Network operator: one name for providers that span several autonomous systems or organisation names (Amazon, Google, Microsoft, Alibaba, Oracle, OVHcloud, Hetzner, Contabo, Akamai, Tencent), else the organisation without its legal suffix. Empty for residential peers and when unknown' CODEC(ZSTD(1)),
    `ipv6` Bool COMMENT 'The address is IPv6' CODEC(ZSTD(1)),
    `dialled` Bool COMMENT 'Our sentries dialled the peer (an outbound connection), so it is publicly reachable' CODEC(ZSTD(1)),
    `inbound` Bool COMMENT 'The peer connected to our sentries (an inbound connection)' CODEC(ZSTD(1)),
    `quic_connected` Bool COMMENT 'Connected to over QUIC' CODEC(ZSTD(1)),
    `quic_listen` Bool COMMENT 'Advertises a QUIC listen address in identify' CODEC(ZSTD(1)),
    `enr_ip_mismatch` Nullable(Bool) COMMENT 'The address in the peer ENR differs from the one our sentries connected to, NULL unless both are known' CODEC(ZSTD(1)),
    `custody_group_count` Nullable(UInt8) COMMENT 'Custody group count from the peer metadata responses, else from its ENR, NULL when unknown' CODEC(ZSTD(1)),
    `latency_ms` Nullable(UInt32) COMMENT 'Median heartbeat latency to our sentries in ms, NULL without heartbeat latencies' CODEC(ZSTD(1)),
    `status_count` UInt32 COMMENT 'Status exchanges with our sentries in the day' CODEC(ZSTD(1)),
    `fork_digest` LowCardinality(String) COMMENT 'Fork digest of the peer latest status, else of its ENR, empty when neither is known' CODEC(ZSTD(1)),
    `head_lag_slots` Nullable(Int32) COMMENT 'Median of the slots the peer head trailed the head of the sentry it exchanged status with, NULL without status exchanges' CODEC(ZSTD(1)),
    `finalized_lag_epochs` Nullable(Int32) COMMENT 'Median of the epochs the peer finalized checkpoint trailed that of the sentry it exchanged status with, NULL without status exchanges' CODEC(ZSTD(1)),
    `earliest_available_slot` Nullable(UInt32) COMMENT 'Earliest slot the peer serves blocks from in its latest status (0 for full history), NULL when it sent none (status v1)' CODEC(ZSTD(1)),
    `served_history_slots` Nullable(UInt32) COMMENT 'Slots of block history the peer serves at its latest status: the head of the sentry it exchanged status with minus earliest_available_slot. The spec minimum is 1056768 slots (33024 epochs)' CODEC(ZSTD(1)),
    INDEX idx_peer_id `peer_id` TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `peer_id_unique_key`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'One row per consensus peer per day, from our libp2p sentries (heartbeats, identify, metadata and status exchanges) and the discovery crawler (ENRs). Sentry values win over ENR values. Crawlers, probes, light clients and our own sentries are excluded. Holds no addresses. Residential peers are country-only: city and autonomous system are kept for hosting peers only.';

CREATE TABLE int_peer_daily ON CLUSTER '{cluster}' AS int_peer_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_peer_daily_local,
    cityHash64(`day_start_date`, `peer_id_unique_key`)
);

CREATE TABLE fct_peer_sync_status_by_cl_client_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent: lighthouse, lodestar, nimbus, prysm, teku, grandine, caplin (erigon), silver or ethlambda, unknown when the agent was never seen, other for every other agent' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers our sentries saw in the day' CODEC(ZSTD(1)),
    `status_peer_count` UInt32 COMMENT 'Peers that exchanged status with our sentries, the population of the head, finalized and fork digest buckets' CODEC(ZSTD(1)),
    `head_lag_0_1_peer_count` UInt32 COMMENT 'Peers whose head trailed our sentries by at most 1 slot (median over the day)' CODEC(ZSTD(1)),
    `head_lag_2_32_peer_count` UInt32 COMMENT 'Peers whose head trailed our sentries by 2 to 32 slots' CODEC(ZSTD(1)),
    `head_lag_over_32_peer_count` UInt32 COMMENT 'Peers whose head trailed our sentries by more than 32 slots (syncing or stuck)' CODEC(ZSTD(1)),
    `finalized_lag_over_1_peer_count` UInt32 COMMENT 'Peers whose finalized checkpoint trailed our sentries by more than 1 epoch (median over the day)' CODEC(ZSTD(1)),
    `other_fork_digest_peer_count` UInt32 COMMENT 'Peers whose latest status carried another fork digest than the most common one among all peers that day' CODEC(ZSTD(1)),
    `history_known_peer_count` UInt32 COMMENT 'Peers that sent an earliest available slot (status v2), the population of the history buckets' CODEC(ZSTD(1)),
    `history_full_peer_count` UInt32 COMMENT 'Peers serving blocks from genesis (earliest available slot 0)' CODEC(ZSTD(1)),
    `history_over_min_peer_count` UInt32 COMMENT 'Peers serving more than a day beyond the spec minimum of 33024 epochs of blocks, short of genesis' CODEC(ZSTD(1)),
    `history_min_peer_count` UInt32 COMMENT 'Peers serving the spec minimum of 33024 epochs of blocks, give or take a day' CODEC(ZSTD(1)),
    `history_under_min_peer_count` UInt32 COMMENT 'Peers serving more than a day less than the spec minimum of 33024 epochs of blocks' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peer sync status by client, from status exchanges with our libp2p sentries: head and finalized lag against the sentry, fork digest, and block history served (earliest available slot).';

CREATE TABLE fct_peer_sync_status_by_cl_client_daily ON CLUSTER '{cluster}' AS fct_peer_sync_status_by_cl_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_sync_status_by_cl_client_daily_local,
    cityHash64(`day_start_date`, `client`)
);

CREATE TABLE fct_peer_reachability_by_cl_client_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent: lighthouse, lodestar, nimbus, prysm, teku, grandine, caplin (erigon), silver or ethlambda, unknown when the agent was never seen, other for every other agent' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers our sentries saw in the day' CODEC(ZSTD(1)),
    `dialled_peer_count` UInt32 COMMENT 'Peers our sentries dialled (outbound connections), so publicly reachable' CODEC(ZSTD(1)),
    `inbound_only_peer_count` UInt32 COMMENT 'Peers that only connected to our sentries and were never dialled' CODEC(ZSTD(1)),
    `quic_connected_peer_count` UInt32 COMMENT 'Peers connected to over QUIC' CODEC(ZSTD(1)),
    `quic_listen_peer_count` UInt32 COMMENT 'Peers advertising a QUIC listen address in identify' CODEC(ZSTD(1)),
    `enr_peer_count` UInt32 COMMENT 'Peers our discovery crawler also dialled from their ENR' CODEC(ZSTD(1)),
    `enr_ip_mismatch_peer_count` UInt32 COMMENT 'Of those, peers whose ENR advertises another address than the one our sentries connected to' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peer reachability by client: peers our libp2p sentries dialled or that only connected in, QUIC, and ENR address mismatches.';

CREATE TABLE fct_peer_reachability_by_cl_client_daily ON CLUSTER '{cluster}' AS fct_peer_reachability_by_cl_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_reachability_by_cl_client_daily_local,
    cityHash64(`day_start_date`, `client`)
);

CREATE TABLE int_peer_daily_order_by_peer_id_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `peer_id_unique_key` Int64 COMMENT 'Hash of the peer ID and network, the key shared by the libp2p tables' CODEC(ZSTD(1)),
    `peer_id` String COMMENT 'libp2p peer ID, empty when the peer was only seen before peer IDs were recorded (April 2026)' CODEC(ZSTD(1)),
    `seen_by_sentry` Bool COMMENT 'Our libp2p sentries were connected to the peer in the day. False on both sources means the peer was dropped from the day on a recount' CODEC(ZSTD(1)),
    `seen_by_enr` Bool COMMENT 'Our discovery crawler dialled the peer from its ENR in the day' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent, else its ENR name (erigon reported as caplin), unknown when neither names one, other for unparseable names' CODEC(ZSTD(1)),
    `version` LowCardinality(String) COMMENT 'Version the agent states (semver with any pre-release suffix), empty when it states none' CODEC(ZSTD(1)),
    `agent_version` String COMMENT 'Version field of the agent as reported, for Caplin its platform field' CODEC(ZSTD(1)),
    `continent_code` LowCardinality(String) COMMENT 'Continent code of the address, empty when not geolocated' CODEC(ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code of the address, empty when not geolocated' CODEC(ZSTD(1)),
    `country` LowCardinality(String) COMMENT 'Country name of the address, empty when not geolocated' CODEC(ZSTD(1)),
    `city` LowCardinality(String) COMMENT 'City of the address for hosting peers, empty for residential peers (network_type other or unknown) and when not geolocated to a city' CODEC(ZSTD(1)),
    `autonomous_system_number` UInt32 COMMENT 'Autonomous system number of the address for hosting peers, 0 for residential peers and when unknown' CODEC(ZSTD(1)),
    `autonomous_system_organization` LowCardinality(String) COMMENT 'Autonomous system organisation of the address for hosting peers, empty for residential peers and when unknown' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs), unknown when not geolocated' CODEC(ZSTD(1)),
    `provider` LowCardinality(String) COMMENT 'Network operator: one name for providers that span several autonomous systems or organisation names (Amazon, Google, Microsoft, Alibaba, Oracle, OVHcloud, Hetzner, Contabo, Akamai, Tencent), else the organisation without its legal suffix. Empty for residential peers and when unknown' CODEC(ZSTD(1)),
    `ipv6` Bool COMMENT 'The address is IPv6' CODEC(ZSTD(1)),
    `dialled` Bool COMMENT 'Our sentries dialled the peer (an outbound connection), so it is publicly reachable' CODEC(ZSTD(1)),
    `inbound` Bool COMMENT 'The peer connected to our sentries (an inbound connection)' CODEC(ZSTD(1)),
    `quic_connected` Bool COMMENT 'Connected to over QUIC' CODEC(ZSTD(1)),
    `quic_listen` Bool COMMENT 'Advertises a QUIC listen address in identify' CODEC(ZSTD(1)),
    `enr_ip_mismatch` Nullable(Bool) COMMENT 'The address in the peer ENR differs from the one our sentries connected to, NULL unless both are known' CODEC(ZSTD(1)),
    `custody_group_count` Nullable(UInt8) COMMENT 'Custody group count from the peer metadata responses, else from its ENR, NULL when unknown' CODEC(ZSTD(1)),
    `latency_ms` Nullable(UInt32) COMMENT 'Median heartbeat latency to our sentries in ms, NULL without heartbeat latencies' CODEC(ZSTD(1)),
    `status_count` UInt32 COMMENT 'Status exchanges with our sentries in the day' CODEC(ZSTD(1)),
    `fork_digest` LowCardinality(String) COMMENT 'Fork digest of the peer latest status, else of its ENR, empty when neither is known' CODEC(ZSTD(1)),
    `head_lag_slots` Nullable(Int32) COMMENT 'Median of the slots the peer head trailed the head of the sentry it exchanged status with, NULL without status exchanges' CODEC(ZSTD(1)),
    `finalized_lag_epochs` Nullable(Int32) COMMENT 'Median of the epochs the peer finalized checkpoint trailed that of the sentry it exchanged status with, NULL without status exchanges' CODEC(ZSTD(1)),
    `earliest_available_slot` Nullable(UInt32) COMMENT 'Earliest slot the peer serves blocks from in its latest status (0 for full history), NULL when it sent none (status v1)' CODEC(ZSTD(1)),
    `served_history_slots` Nullable(UInt32) COMMENT 'Slots of block history the peer serves at its latest status: the head of the sentry it exchanged status with minus earliest_available_slot. The spec minimum is 1056768 slots (33024 epochs)' CODEC(ZSTD(1)),
    INDEX idx_peer_id `peer_id` TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`peer_id`, `day_start_date`, `peer_id_unique_key`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'int_peer_daily ordered by peer ID, for reading one peer across days. One row per consensus peer per day, from our libp2p sentries (heartbeats, identify, metadata and status exchanges) and the discovery crawler (ENRs). Holds no addresses. Residential peers are country-only: city and autonomous system are kept for hosting peers only.';

CREATE TABLE int_peer_daily_order_by_peer_id ON CLUSTER '{cluster}' AS int_peer_daily_order_by_peer_id_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_peer_daily_order_by_peer_id_local,
    cityHash64(`peer_id`)
);

CREATE TABLE fct_peer_count_by_source_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Consensus peers our libp2p sentries or our discovery crawler saw in the day' CODEC(ZSTD(1)),
    `sentry_peer_count` UInt32 COMMENT 'Peers our libp2p sentries were connected to' CODEC(ZSTD(1)),
    `enr_peer_count` UInt32 COMMENT 'Peers our discovery crawler dialled from their ENR' CODEC(ZSTD(1)),
    `sentry_and_enr_peer_count` UInt32 COMMENT 'Peers both sources saw' CODEC(ZSTD(1)),
    `dialled_peer_count` UInt32 COMMENT 'Peers our sentries dialled (outbound connections), so publicly reachable' CODEC(ZSTD(1)),
    `inbound_only_peer_count` UInt32 COMMENT 'Peers that only connected to our sentries and were never dialled' CODEC(ZSTD(1)),
    `quic_peer_count` UInt32 COMMENT 'Peers connected to over QUIC or advertising a QUIC listen address' CODEC(ZSTD(1)),
    `ipv6_peer_count` UInt32 COMMENT 'Peers on an IPv6 address' CODEC(ZSTD(1)),
    `hosting_peer_count` UInt32 COMMENT 'Peers on a cloud or hosting provider network' CODEC(ZSTD(1)),
    `located_peer_count` UInt32 COMMENT 'Peers with a known autonomous system, the population of hosting_peer_count' CODEC(ZSTD(1)),
    `custody_known_peer_count` UInt32 COMMENT 'Peers whose custody group count is known from a metadata response or their ENR' CODEC(ZSTD(1)),
    `custody_4_peer_count` UInt32 COMMENT 'Peers custodying 4 or fewer groups (the minimum)' CODEC(ZSTD(1)),
    `custody_5_8_peer_count` UInt32 COMMENT 'Peers custodying 5 to 8 groups' CODEC(ZSTD(1)),
    `custody_9_127_peer_count` UInt32 COMMENT 'Peers custodying 9 to 127 groups' CODEC(ZSTD(1)),
    `custody_128_peer_count` UInt32 COMMENT 'Peers custodying all 128 groups (supernodes)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peer totals by source: peers our libp2p sentries and our discovery crawler saw, and how many both saw, with reachability, hosting and custody counts. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_count_by_source_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_source_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_source_daily_local,
    cityHash64(`day_start_date`)
);
