---
table: fct_custody_probe_by_client_daily
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
  - custody
  - peerdas
dependencies:
  - "{{transformation}}.int_custody_probe"
  - "{{transformation}}.dim_client_release"
  - "{{transformation}}.dim_client_commit"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and NULL percentiles.
-- Data column custody probes of the day by the probed peer's client and release (its latest agent in the day).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    probes AS (
        SELECT
            toDate(probe_date_time) AS day_start_date,
            peer_id_unique_key,
            probe_date_time,
            result,
            response_time_ms,
            lower(meta_peer_implementation) AS implementation,
            meta_peer_version AS agent_version,
            meta_peer_platform AS agent_platform
        FROM {{ index .dep "{{transformation}}" "int_custody_probe" "helpers" "from" }} FINAL
        WHERE probe_date_time >= (SELECT min_day FROM day_bounds)
          AND probe_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    probed_peers AS (
        SELECT
            day_start_date,
            peer_id_unique_key,
            toDateTime(day_start_date) + INTERVAL 1 DAY AS seen_at,
            argMax((implementation, agent_version, agent_platform), (implementation != '', probe_date_time, agent_version, agent_platform)) AS agent,
            multiIf(
                agent.1 = '', 'unknown',
                agent.1 = 'erigon', 'caplin',
                match(agent.1, '^[a-z][a-z0-9-]*$'), agent.1,
                'other'
            ) AS client,
            if(agent.1 = 'erigon', agent.3, agent.2) AS version_text,
            extract(version_text, '^v?\\d+\\.\\d+\\.\\d+(?:-(?:rc|beta|alpha)[.0-9]*)?') AS semver,
            multiIf(
                agent.1 = 'lodestar', extract(agent.3, '^([0-9a-f]{7,40})$'),
                extract(version_text, '[-+]g?([0-9a-f]{7,40})(?:-[a-z]+)?$')
            ) AS commit
        FROM probes
        GROUP BY day_start_date, peer_id_unique_key
        HAVING NOT (agent.1 IN ('tysm', 'hermes', 'nebula', 'eth2-crawler', 'enrscout', 'oft-probe', 'migalabs-kumo', 'xrpc-basel1lc', 'geth-blsync', 'nimbus-verified-proxy') OR match(agent.1, 'crawler|probe|scout|armiarma'))
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
        FROM (SELECT DISTINCT client, commit, seen_at FROM probed_peers WHERE commit != '') AS l
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
        FROM (SELECT DISTINCT client, commit, seen_at FROM probed_peers WHERE length(commit) >= 4) AS l
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
        FROM probed_peers AS p
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
    toUInt32(sum(peer_count)) AS peer_count,
    toUInt32(sum(probe_count)) AS probe_count,
    toUInt32(sum(success_count)) AS success_count,
    toUInt32(sum(missing_count)) AS missing_count,
    toUInt32(sum(failure_count)) AS failure_count,
    toUInt32(sum(missing_peer_count)) AS missing_peer_count,
    max(response_time_p50_ms) AS response_time_p50_ms,
    max(response_time_p90_ms) AS response_time_p90_ms
FROM (
    SELECT
        p.day_start_date AS day_start_date,
        client,
        version,
        release_status,
        uniqExact(p.peer_id_unique_key) AS peer_count,
        count() AS probe_count,
        countIf(result = 'success') AS success_count,
        countIf(result = 'missing') AS missing_count,
        countIf(result = 'failure') AS failure_count,
        uniqExactIf(p.peer_id_unique_key, result = 'missing') AS missing_peer_count,
        if(isNaN(quantileIf(0.5)(response_time_ms, result = 'success')), NULL, quantileIf(0.5)(response_time_ms, result = 'success')) AS response_time_p50_ms,
        if(isNaN(quantileIf(0.9)(response_time_ms, result = 'success')), NULL, quantileIf(0.9)(response_time_ms, result = 'success')) AS response_time_p90_ms
    FROM probes AS p
    GLOBAL INNER JOIN resolved AS r ON p.day_start_date = r.day_start_date AND p.peer_id_unique_key = r.peer_id_unique_key
    GROUP BY day_start_date, client, version, release_status

    UNION ALL

    SELECT
        day_start_date,
        client,
        version,
        release_status,
        0 AS peer_count,
        0 AS probe_count,
        0 AS success_count,
        0 AS missing_count,
        0 AS failure_count,
        0 AS missing_peer_count,
        NULL AS response_time_p50_ms,
        NULL AS response_time_p90_ms
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, client, version, release_status
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
