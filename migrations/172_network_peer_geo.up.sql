CREATE TABLE fct_peer_count_by_country_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `continent_code` LowCardinality(String) COMMENT 'Continent code of the address, unknown when not geolocated' CODEC(ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code of the address, unknown when not geolocated' CODEC(ZSTD(1)),
    `country` LowCardinality(String) COMMENT 'Country name, Unknown when not geolocated' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Consensus peers our libp2p sentries or our discovery crawler saw in the day' CODEC(ZSTD(1)),
    `sentry_peer_count` UInt32 COMMENT 'Peers our libp2p sentries were connected to' CODEC(ZSTD(1)),
    `enr_peer_count` UInt32 COMMENT 'Peers our discovery crawler dialled from their ENR' CODEC(ZSTD(1)),
    `dialled_peer_count` UInt32 COMMENT 'Peers our sentries dialled (outbound connections), so publicly reachable' CODEC(ZSTD(1)),
    `inbound_only_peer_count` UInt32 COMMENT 'Peers that only connected to our sentries and were never dialled' CODEC(ZSTD(1)),
    `quic_peer_count` UInt32 COMMENT 'Peers connected to over QUIC or advertising a QUIC listen address' CODEC(ZSTD(1)),
    `hosting_peer_count` UInt32 COMMENT 'Peers on a cloud or hosting provider network' CODEC(ZSTD(1)),
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
    (`day_start_date`, `continent_code`, `country_code`, `country`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peers by country, from our libp2p sentries and our discovery crawler, with reachability, hosting and custody counts. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_count_by_country_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_country_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_country_daily_local,
    cityHash64(`day_start_date`, `continent_code`, `country_code`, `country`)
);

CREATE TABLE fct_peer_count_by_asn_country_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `autonomous_system_number` UInt32 COMMENT 'Autonomous system number of the address, 0 for unknown and for residential autonomous systems with under 3 peers in the country' CODEC(ZSTD(1)),
    `autonomous_system_organization` LowCardinality(String) COMMENT 'Autonomous system organisation, unknown when not geolocated, other for residential autonomous systems with under 3 peers in the country' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs), unknown when not geolocated' CODEC(ZSTD(1)),
    `continent_code` LowCardinality(String) COMMENT 'Continent code of the address, unknown when not geolocated' CODEC(ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code of the address, unknown when not geolocated' CODEC(ZSTD(1)),
    `country` LowCardinality(String) COMMENT 'Country name, Unknown when not geolocated' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Consensus peers our libp2p sentries or our discovery crawler saw in the day' CODEC(ZSTD(1)),
    `sentry_peer_count` UInt32 COMMENT 'Peers our libp2p sentries were connected to' CODEC(ZSTD(1)),
    `enr_peer_count` UInt32 COMMENT 'Peers our discovery crawler dialled from their ENR' CODEC(ZSTD(1)),
    `dialled_peer_count` UInt32 COMMENT 'Peers our sentries dialled (outbound connections), so publicly reachable' CODEC(ZSTD(1)),
    `inbound_only_peer_count` UInt32 COMMENT 'Peers that only connected to our sentries and were never dialled' CODEC(ZSTD(1)),
    `quic_peer_count` UInt32 COMMENT 'Peers connected to over QUIC or advertising a QUIC listen address' CODEC(ZSTD(1)),
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
    (`day_start_date`, `autonomous_system_number`, `autonomous_system_organization`, `network_type`, `continent_code`, `country_code`, `country`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peers by autonomous system and country, residential and hosting, from our libp2p sentries and our discovery crawler, with reachability and custody counts. Not crossed with city or client. Residential (network type other or unknown) autonomous system and country cells under 3 peers roll into autonomous system 0 (other) within the country. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_count_by_asn_country_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_asn_country_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_asn_country_daily_local,
    cityHash64(`day_start_date`, `autonomous_system_number`, `autonomous_system_organization`, `network_type`, `continent_code`, `country_code`, `country`)
);

CREATE TABLE fct_peer_count_by_asn_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `autonomous_system_number` UInt32 COMMENT 'Autonomous system number of the address, 0 for unknown and for residential autonomous systems with under 3 peers in the day' CODEC(ZSTD(1)),
    `autonomous_system_organization` LowCardinality(String) COMMENT 'Autonomous system organisation, unknown when not geolocated, other for residential autonomous systems with under 3 peers in the day' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs), unknown when not geolocated' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Consensus peers our libp2p sentries or our discovery crawler saw in the day' CODEC(ZSTD(1)),
    `sentry_peer_count` UInt32 COMMENT 'Peers our libp2p sentries were connected to' CODEC(ZSTD(1)),
    `enr_peer_count` UInt32 COMMENT 'Peers our discovery crawler dialled from their ENR' CODEC(ZSTD(1)),
    `dialled_peer_count` UInt32 COMMENT 'Peers our sentries dialled (outbound connections), so publicly reachable' CODEC(ZSTD(1)),
    `inbound_only_peer_count` UInt32 COMMENT 'Peers that only connected to our sentries and were never dialled' CODEC(ZSTD(1)),
    `quic_peer_count` UInt32 COMMENT 'Peers connected to over QUIC or advertising a QUIC listen address' CODEC(ZSTD(1)),
    `custody_known_peer_count` UInt32 COMMENT 'Peers whose custody group count is known from a metadata response or their ENR' CODEC(ZSTD(1)),
    `custody_4_peer_count` UInt32 COMMENT 'Peers custodying 4 or fewer groups (the minimum)' CODEC(ZSTD(1)),
    `custody_5_8_peer_count` UInt32 COMMENT 'Peers custodying 5 to 8 groups' CODEC(ZSTD(1)),
    `custody_9_127_peer_count` UInt32 COMMENT 'Peers custodying 9 to 127 groups' CODEC(ZSTD(1)),
    `custody_128_peer_count` UInt32 COMMENT 'Peers custodying all 128 groups (supernodes)' CODEC(ZSTD(1)),
    `autonomous_system_count` UInt32 COMMENT 'Autonomous systems in the row: 1, the number of residential systems folded into autonomous system 0 (other), each with 1 or 2 peers, or 0 for unknown' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `autonomous_system_number`, `autonomous_system_organization`, `network_type`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peers by autonomous system, residential and hosting, from our libp2p sentries and our discovery crawler, with reachability and custody counts. Residential autonomous systems under 3 peers in the day roll into autonomous system 0 (other), which keeps how many systems it holds. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_count_by_asn_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_asn_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_asn_daily_local,
    cityHash64(`day_start_date`, `autonomous_system_number`, `autonomous_system_organization`, `network_type`)
);

CREATE TABLE fct_peer_count_by_cl_client_city_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code of the address, unknown when not geolocated' CODEC(ZSTD(1)),
    `city` LowCardinality(String) COMMENT 'City of the address, unknown when not geolocated to a city, other for residential city and client cells with under 3 peers' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs), unknown when not geolocated' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client from the peer agent: lighthouse, lodestar, nimbus, prysm, teku, grandine, caplin (erigon), silver or ethlambda, unknown when the agent was never seen, other for every other agent' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Consensus peers our libp2p sentries or our discovery crawler saw in the day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `country_code`, `city`, `network_type`, `client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily consensus peers by country, city, network type and client, residential and hosting, from our libp2p sentries and our discovery crawler. Not crossed with autonomous system or version. Residential (network type other or unknown) city and client cells under 3 peers roll into city other within the country. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_count_by_cl_client_city_daily ON CLUSTER '{cluster}' AS fct_peer_count_by_cl_client_city_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_count_by_cl_client_city_daily_local,
    cityHash64(`day_start_date`, `country_code`, `city`, `network_type`, `client`)
);

CREATE TABLE fct_custody_probe_by_country_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `continent_code` LowCardinality(String) COMMENT 'Continent code of the address, unknown when not geolocated' CODEC(ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code of the address, unknown when not geolocated' CODEC(ZSTD(1)),
    `country` LowCardinality(String) COMMENT 'Country name, Unknown when not geolocated' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers probed in the day' CODEC(ZSTD(1)),
    `probe_count` UInt32 COMMENT 'Custody probes' CODEC(ZSTD(1)),
    `success_count` UInt32 COMMENT 'Probes the peer answered with the column' CODEC(ZSTD(1)),
    `missing_count` UInt32 COMMENT 'Probes the peer answered without the column' CODEC(ZSTD(1)),
    `failure_count` UInt32 COMMENT 'Probes that errored or timed out' CODEC(ZSTD(1)),
    `missing_peer_count` UInt32 COMMENT 'Peers with at least one missing probe' CODEC(ZSTD(1)),
    `response_time_p50_ms` Nullable(Float64) COMMENT 'Median response time of successful probes in ms, NULL without any' CODEC(ZSTD(1)),
    `response_time_p90_ms` Nullable(Float64) COMMENT '90th percentile response time of successful probes in ms, NULL without any' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `continent_code`, `country_code`, `country`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily data column custody probes by the probed peer country (its latest location in the day). Crawlers and probes are excluded.';

CREATE TABLE fct_custody_probe_by_country_daily ON CLUSTER '{cluster}' AS fct_custody_probe_by_country_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_custody_probe_by_country_daily_local,
    cityHash64(`day_start_date`, `continent_code`, `country_code`, `country`)
);

CREATE TABLE fct_custody_probe_by_asn_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `autonomous_system_number` UInt32 COMMENT 'Autonomous system number of the address, 0 when unknown' CODEC(ZSTD(1)),
    `autonomous_system_organization` LowCardinality(String) COMMENT 'Autonomous system organisation, unknown when not geolocated' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs), unknown when not geolocated' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Peers probed in the day' CODEC(ZSTD(1)),
    `probe_count` UInt32 COMMENT 'Custody probes' CODEC(ZSTD(1)),
    `success_count` UInt32 COMMENT 'Probes the peer answered with the column' CODEC(ZSTD(1)),
    `missing_count` UInt32 COMMENT 'Probes the peer answered without the column' CODEC(ZSTD(1)),
    `failure_count` UInt32 COMMENT 'Probes that errored or timed out' CODEC(ZSTD(1)),
    `missing_peer_count` UInt32 COMMENT 'Peers with at least one missing probe' CODEC(ZSTD(1)),
    `response_time_p50_ms` Nullable(Float64) COMMENT 'Median response time of successful probes in ms, NULL without any' CODEC(ZSTD(1)),
    `response_time_p90_ms` Nullable(Float64) COMMENT '90th percentile response time of successful probes in ms, NULL without any' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `autonomous_system_number`, `autonomous_system_organization`, `network_type`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily data column custody probes by the probed peer autonomous system (its latest location in the day), residential and hosting. Crawlers and probes are excluded.';

CREATE TABLE fct_custody_probe_by_asn_daily ON CLUSTER '{cluster}' AS fct_custody_probe_by_asn_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_custody_probe_by_asn_daily_local,
    cityHash64(`day_start_date`, `autonomous_system_number`, `autonomous_system_organization`, `network_type`)
);

CREATE TABLE dim_asn_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `autonomous_system_number` UInt32 COMMENT 'Autonomous system number of the address, 0 when unknown' CODEC(ZSTD(1)),
    `autonomous_system_organization` LowCardinality(String) COMMENT 'Autonomous system organisation on the last day it had peers' CODEC(ZSTD(1)),
    `provider` LowCardinality(String) COMMENT 'Network operator: one name for providers that span several autonomous systems or organisation names (Amazon, Google, Microsoft, Alibaba, Oracle, OVHcloud, Hetzner, Contabo, Akamai, Tencent), else the organisation without its legal suffix, empty when unknown' CODEC(ZSTD(1)),
    `network_type` LowCardinality(String) COMMENT 'hosting for cloud and hosting providers (known ASNs and organisation names), other for the rest (mostly residential and business ISPs)' CODEC(ZSTD(1)),
    `last_seen_date` Date COMMENT 'Last day our consensus peers were on the autonomous system' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `last_seen_date`
)
ORDER BY
    (`autonomous_system_number`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Autonomous systems our consensus peers were on, residential and hosting, with organisation, provider and network type as of the last day they had peers.';

CREATE TABLE dim_asn ON CLUSTER '{cluster}' AS dim_asn_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    dim_asn_local,
    cityHash64(`autonomous_system_number`)
);

CREATE TABLE fct_peer_concentration_cl_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Consensus peers our libp2p sentries or our discovery crawler saw in the day' CODEC(ZSTD(1)),
    `located_peer_count` UInt32 COMMENT 'Peers with a known autonomous system, the population of hosting_peer_count' CODEC(ZSTD(1)),
    `hosting_peer_count` UInt32 COMMENT 'Peers on a cloud or hosting provider network' CODEC(ZSTD(1)),
    `ipv6_peer_count` UInt32 COMMENT 'Peers on an IPv6 address' CODEC(ZSTD(1)),
    `country_count` UInt32 COMMENT 'Countries with peers' CODEC(ZSTD(1)),
    `third_country_count` UInt32 COMMENT 'Fewest countries that hold a third of the peers with a known country' CODEC(ZSTD(1)),
    `half_country_count` UInt32 COMMENT 'Fewest countries that hold half of the peers with a known country' CODEC(ZSTD(1)),
    `autonomous_system_count` UInt32 COMMENT 'Autonomous systems with peers, counting each residential system folded into autonomous system 0 (other) on its own' CODEC(ZSTD(1)),
    `top_1_asn_peer_count` UInt32 COMMENT 'Peers in the largest autonomous system' CODEC(ZSTD(1)),
    `top_5_asn_peer_count` UInt32 COMMENT 'Peers in the 5 largest autonomous systems' CODEC(ZSTD(1)),
    `top_10_asn_peer_count` UInt32 COMMENT 'Peers in the 10 largest autonomous systems' CODEC(ZSTD(1)),
    `third_asn_count` UInt32 COMMENT 'Fewest autonomous systems that hold a third of the peers with a known autonomous system' CODEC(ZSTD(1)),
    `half_asn_count` UInt32 COMMENT 'Fewest autonomous systems that hold half of the peers with a known autonomous system' CODEC(ZSTD(1)),
    `africa_peer_count` UInt32 COMMENT 'Peers in Africa (AF)' CODEC(ZSTD(1)),
    `antarctica_peer_count` UInt32 COMMENT 'Peers in Antarctica (AN)' CODEC(ZSTD(1)),
    `asia_peer_count` UInt32 COMMENT 'Peers in Asia (AS)' CODEC(ZSTD(1)),
    `europe_peer_count` UInt32 COMMENT 'Peers in Europe (EU)' CODEC(ZSTD(1)),
    `north_america_peer_count` UInt32 COMMENT 'Peers in North America (NA)' CODEC(ZSTD(1)),
    `oceania_peer_count` UInt32 COMMENT 'Peers in Oceania (OC)' CODEC(ZSTD(1)),
    `south_america_peer_count` UInt32 COMMENT 'Peers in South America (SA)' CODEC(ZSTD(1)),
    `unknown_continent_peer_count` UInt32 COMMENT 'Peers not geolocated to a continent' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily concentration of consensus peers our libp2p sentries or our discovery crawler saw: countries and autonomous systems, the largest systems, the fewest countries and systems holding a third and half of the peers, and peers per continent. Shares are of peers with a known country or system. Crawlers, probes, light clients and our own sentries are excluded.';

CREATE TABLE fct_peer_concentration_cl_daily ON CLUSTER '{cluster}' AS fct_peer_concentration_cl_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_peer_concentration_cl_daily_local,
    cityHash64(`day_start_date`)
);
