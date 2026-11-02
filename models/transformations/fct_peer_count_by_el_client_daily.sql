---
table: fct_peer_count_by_el_client_daily
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 1m"
tags:
  - daily
  - peer
  - client
  - release
  - discovery
dependencies:
  - "{{external}}.node_record_execution"
  - "{{transformation}}.dim_client_release"
  - "{{transformation}}.dim_client_commit"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and NULL percentiles.
-- Execution nodes our discovery crawler found in the day by client and release, parsed from their devp2p name.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    el_rows AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            node_id,
            event_date_time,
            lower(implementation) AS implementation,
            name,
            genesis,
            fork_id_hash,
            fork_id_next,
            ip,
            geo_country_code AS country_code,
            geo_country AS country,
            geo_continent_code AS continent_code,
            geo_autonomous_system_number AS asn,
            geo_autonomous_system_organization AS asn_organization
        FROM {{ index .dep "{{external}}" "node_record_execution" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),
    network_genesis AS (
        SELECT day_start_date, topK(1)(genesis)[1] AS genesis
        FROM el_rows
        GROUP BY day_start_date
    ),
    el_node_days AS (
        SELECT
            day_start_date,
            node_id,
            argMax(
                (implementation, name, fork_id_hash, fork_id_next, ip, country_code, country, continent_code, asn, asn_organization),
                (event_date_time, name, ifNull(ip, toIPv6('::')))
            ) AS record
        FROM el_rows
        WHERE (day_start_date, genesis) GLOBAL IN (SELECT day_start_date, genesis FROM network_genesis)
        GROUP BY day_start_date, node_id
    ),
    el_peers AS (
        SELECT
            day_start_date,
            node_id,
            toDateTime(day_start_date) + INTERVAL 1 DAY AS seen_at,
            record.1 AS implementation,
            multiIf(
                record.1 = 'nimbus', 'nimbusel',
                match(record.1, '^[a-z][a-z0-9_-]*$'), record.1,
                'other'
            ) AS client,
            arrayFirst(s -> match(s, '\\d+\\.\\d+\\.\\d+'), splitByChar('/', record.2)) AS version_segment,
            extract(version_segment, '^v?\\d+\\.\\d+\\.\\d+(?:-(?:rc|beta|alpha)[.0-9]*)?') AS semver,
            extract(version_segment, '[-+]g?([0-9a-f]{7,40})(?:-[a-z]+)?$') AS commit,
            record.3 AS fork_id_hash,
            record.4 AS fork_id_next,
            record.5 AS ip,
            record.6 AS country_code,
            record.7 AS country,
            record.8 AS continent_code,
            record.9 AS asn,
            record.10 AS asn_organization
        FROM el_node_days
        WHERE record.1 != '' AND NOT match(lower(record.2), 'probe|scout|sniff|leech|observer|listener|crawler|xatu')
    ),
    releases AS (
        SELECT
            client,
            version,
            commit_sha,
            release_date,
            -- Stable before pre-release, then the numerically highest version.
            (NOT prerelease, arrayMap(x -> toUInt32OrZero(x), extractAll(version, '\\d+'))) AS version_rank
        FROM {{ index .dep "{{transformation}}" "dim_client_release" "helpers" "from" }} FINAL
        -- Fail until both dimension tables have run once, so a fresh deployment retries instead of storing unresolved rows.
        WHERE throwIf((SELECT count() FROM {{ index .dep "{{transformation}}" "dim_client_release" "helpers" "from" }}) = 0, 'dim_client_release is empty') = 0
          AND throwIf((SELECT count() FROM {{ index .dep "{{transformation}}" "dim_client_commit" "helpers" "from" }}) = 0, 'dim_client_commit is empty') = 0
    ),
    -- One row per release and commit prefix length, so a prefix lookup is an equality join.
    release_prefixes AS (
        SELECT
            client,
            substring(commit_sha, 1, prefix_length) AS commit_prefix,
            commit_sha,
            version,
            release_date,
            version_rank
        FROM releases
        ARRAY JOIN arrayConcat(range(4, 13), [40]) AS prefix_length
    ),
    -- Several releases on a prefix keep only those published by the end of the day, like Lab's slot page.
    commit_releases AS (
        SELECT
            l.client AS release_client,
            l.commit AS release_commit,
            l.seen_at AS release_seen_at,
            uniqExact(r.commit_sha) AS commit_count,
            uniqExactIf(r.commit_sha, r.release_date IS NULL OR r.release_date <= l.seen_at) AS published_commit_count,
            argMax(r.version, (r.version_rank, r.version)) AS any_version,
            argMaxIf(r.version, (r.version_rank, r.version), r.release_date IS NULL OR r.release_date <= l.seen_at) AS published_version
        FROM (SELECT DISTINCT client, commit, seen_at FROM el_peers WHERE commit != '') AS l
        GLOBAL LEFT JOIN release_prefixes AS r ON l.client = r.client AND l.commit = r.commit_prefix
        GROUP BY release_client, release_commit, release_seen_at
    ),
    -- A commit no release tag points at takes the release it builds on from dim_client_commit (default and release
    -- branch commits). Prefixes under 7 characters also need a commit from the 180 days before.
    commit_branches AS (
        SELECT
            l.client AS branch_client,
            l.commit AS branch_commit,
            l.seen_at AS branch_seen_at,
            uniqExact(c.commit_sha) AS branch_commit_count,
            any(c.base_version) AS branch_base_version
        FROM (SELECT DISTINCT client, commit, seen_at FROM el_peers WHERE length(commit) >= 4) AS l
        GLOBAL INNER JOIN (
            SELECT client, commit_sha, commit_date, base_version FROM {{ index .dep "{{transformation}}" "dim_client_commit" "helpers" "from" }} FINAL
        ) AS c ON l.client = c.client AND substring(l.commit, 1, 4) = substring(c.commit_sha, 1, 4)
        WHERE startsWith(c.commit_sha, l.commit)
          AND c.commit_date <= l.seen_at
          AND (length(l.commit) >= 7 OR c.commit_date >= l.seen_at - INTERVAL 180 DAY)
        GROUP BY branch_client, branch_commit, branch_seen_at
    ),
    -- Versions without a commit match tags with or without the leading v.
    version_releases AS (
        SELECT
            client AS version_client,
            replaceRegexpOne(version, '^[vV]', '') AS normalized_version,
            argMax(version, (version_rank, version)) AS release_version
        FROM releases
        GROUP BY version_client, normalized_version
    ),
    resolved AS (
        SELECT
            p.*,
            multiIf(
                match(p.commit, '^0+$'), 'commit_zero',
                p.commit != '' AND (c.commit_count = 1 OR c.published_commit_count = 1), 'commit_match',
                p.commit != '' AND c.commit_count > 1, 'commit_ambiguous',
                p.commit != '' AND b.branch_commit_count = 1, 'commit_untagged',
                p.commit != '' AND b.branch_commit_count > 1, 'commit_ambiguous',
                p.commit != '', 'commit_unmatched',
                p.semver != '' AND v.release_version IS NOT NULL, 'version_match',
                p.semver != '', 'version_unmatched',
                'none'
            ) AS release_status,
            ifNull(multiIf(
                release_status = 'commit_match', if(c.commit_count = 1, c.any_version, c.published_version),
                release_status = 'commit_untagged' AND b.branch_base_version != '', b.branch_base_version,
                release_status = 'version_match', v.release_version,
                p.semver
            ), '') AS version
        FROM el_peers AS p
        GLOBAL LEFT JOIN commit_releases AS c
            ON p.client = c.release_client AND p.commit = c.release_commit AND p.seen_at = c.release_seen_at
        GLOBAL LEFT JOIN commit_branches AS b
            ON p.client = b.branch_client AND p.commit = b.branch_commit AND p.seen_at = b.branch_seen_at
        GLOBAL LEFT JOIN version_releases AS v
            ON p.client = v.version_client AND replaceRegexpOne(p.semver, '^[vV]', '') = v.normalized_version
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    client,
    version,
    release_status,
    toUInt32(sum(peer_count)) AS peer_count
FROM (
    SELECT
        day_start_date,
        client,
        version,
        release_status,
        count() AS peer_count
    FROM resolved
    GROUP BY day_start_date, client, version, release_status

    UNION ALL

    SELECT
        day_start_date,
        client,
        version,
        release_status,
        0 AS peer_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, client, version, release_status
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
