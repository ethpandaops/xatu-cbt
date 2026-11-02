---
table: int_block_graffiti_release
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - graffiti
  - client
  - release
  - canonical
dependencies:
  - "{{transformation}}.fct_block_graffiti"
  - "{{transformation}}.dim_client_release"
  - "{{transformation}}.dim_client_commit"
---
-- Resolves each canonical block's graffiti commits and version strings to client releases the way Lab resolves
-- a single slot: a commit prefix matches release commits, several matching commits keep only releases published
-- by the block time, and the stable, highest version wins among tags on the chosen commit.
-- A commit no release tag points at falls back to dim_client_commit (default and release branch commits): if one
-- commit committed by the block time matches, the block takes the release that commit builds on (commit_untagged).
-- Prefixes under 7 characters also need the commit to be from the 180 days before the block, since a 4 character
-- prefix collides with some commit in most repositories' full history. Unresolved blocks keep an empty version
-- and a release status saying why.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        proposer_index,
        entity,
        graffiti_format,
        consensus_client,
        consensus_client_version AS graffiti_version,
        lower(consensus_client_commit) AS consensus_commit,
        execution_client,
        lower(execution_client_commit) AS execution_commit
    FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
releases AS (
    SELECT
        client,
        version,
        commit_sha,
        release_date,
        prerelease,
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
-- 2 character commits (codes_short) match too many releases to resolve and all-zero commits are placeholders,
-- so both are left unresolved.
commit_lookups AS (
    SELECT DISTINCT client, commit, slot_start_date_time
    FROM (
        SELECT consensus_client AS client, consensus_commit AS commit, slot_start_date_time
        FROM blocks
        WHERE length(consensus_commit) >= 4 AND NOT match(consensus_commit, '^0+$')

        UNION ALL

        SELECT execution_client AS client, execution_commit AS commit, slot_start_date_time
        FROM blocks
        WHERE length(execution_commit) >= 4 AND NOT match(execution_commit, '^0+$')
    )
),
branch_commits AS (
    SELECT client, commit_sha, commit_date, base_version
    FROM {{ index .dep "{{transformation}}" "dim_client_commit" "helpers" "from" }} FINAL
),
-- Joined on the first 4 characters, then filtered to the full prefix.
commit_branches AS (
    SELECT
        l.client AS branch_client,
        l.commit AS branch_commit,
        l.slot_start_date_time AS branch_seen_at,
        uniqExact(c.commit_sha) AS branch_commit_count,
        any(c.base_version) AS branch_base_version
    FROM commit_lookups AS l
    GLOBAL INNER JOIN branch_commits AS c
        ON l.client = c.client AND substring(l.commit, 1, 4) = substring(c.commit_sha, 1, 4)
    WHERE startsWith(c.commit_sha, l.commit)
      AND c.commit_date <= l.slot_start_date_time
      AND (length(l.commit) >= 7 OR c.commit_date >= l.slot_start_date_time - INTERVAL 180 DAY)
    GROUP BY branch_client, branch_commit, branch_seen_at
),
commit_releases AS (
    SELECT
        l.client AS client,
        l.commit AS commit,
        l.slot_start_date_time AS seen_at,
        uniqExact(r.commit_sha) AS commit_count,
        uniqExactIf(r.commit_sha, r.release_date IS NULL OR r.release_date <= l.slot_start_date_time) AS published_commit_count,
        argMax(r.version, (r.version_rank, r.version)) AS any_version,
        argMaxIf(r.version, (r.version_rank, r.version), r.release_date IS NULL OR r.release_date <= l.slot_start_date_time) AS published_version
    FROM commit_lookups AS l
    GLOBAL LEFT JOIN release_prefixes AS r
        ON l.client = r.client AND l.commit = r.commit_prefix
    GROUP BY client, commit, seen_at
),
-- Graffiti version strings (v8.2.3) match tags with or without the leading v.
version_releases AS (
    SELECT
        client,
        replaceRegexpOne(version, '^[vV]', '') AS normalized_version,
        argMax(version, (version_rank, version)) AS release_version
    FROM releases
    GROUP BY client, normalized_version
),
resolved AS (
    SELECT
        b.*,
        cl.commit_count AS cl_commit_count,
        cl.published_commit_count AS cl_published_commit_count,
        cl.any_version AS cl_any_version,
        cl.published_version AS cl_published_version,
        clb.branch_commit_count AS cl_branch_commit_count,
        clb.branch_base_version AS cl_branch_version,
        clv.release_version AS cl_version_release,
        el.commit_count AS el_commit_count,
        el.published_commit_count AS el_published_commit_count,
        el.any_version AS el_any_version,
        el.published_version AS el_published_version,
        elb.branch_commit_count AS el_branch_commit_count,
        elb.branch_base_version AS el_branch_version
    FROM blocks AS b
    GLOBAL LEFT JOIN commit_releases AS cl
        ON b.consensus_client = cl.client AND b.consensus_commit = cl.commit AND b.slot_start_date_time = cl.seen_at
    GLOBAL LEFT JOIN commit_releases AS el
        ON b.execution_client = el.client AND b.execution_commit = el.commit AND b.slot_start_date_time = el.seen_at
    GLOBAL LEFT JOIN commit_branches AS clb
        ON b.consensus_client = clb.branch_client AND b.consensus_commit = clb.branch_commit AND b.slot_start_date_time = clb.branch_seen_at
    GLOBAL LEFT JOIN commit_branches AS elb
        ON b.execution_client = elb.branch_client AND b.execution_commit = elb.branch_commit AND b.slot_start_date_time = elb.branch_seen_at
    GLOBAL LEFT JOIN version_releases AS clv
        ON b.consensus_client = clv.client AND replaceRegexpOne(b.graffiti_version, '^[vV]', '') = clv.normalized_version
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    proposer_index,
    entity,
    graffiti_format,
    consensus_client,
    ifNull(multiIf(
        consensus_status = 'commit_match', if(cl_commit_count = 1, cl_any_version, cl_published_version),
        consensus_status = 'commit_untagged', cl_branch_version,
        consensus_status = 'version_match', cl_version_release,
        ''
    ), '') AS consensus_client_version,
    consensus_commit AS consensus_client_commit,
    consensus_status AS consensus_client_release_status,
    execution_client,
    ifNull(multiIf(
        execution_status = 'commit_match', if(el_commit_count = 1, el_any_version, el_published_version),
        execution_status = 'commit_untagged', el_branch_version,
        ''
    ), '') AS execution_client_version,
    execution_commit AS execution_client_commit,
    execution_status AS execution_client_release_status
FROM (
    SELECT
        *,
        multiIf(
            consensus_client = 'unknown', 'none',
            length(consensus_commit) BETWEEN 1 AND 3, 'commit_short',
            match(consensus_commit, '^0+$'), 'commit_zero',
            consensus_commit != '' AND (cl_commit_count = 1 OR cl_published_commit_count = 1), 'commit_match',
            consensus_commit != '' AND cl_commit_count > 1, 'commit_ambiguous',
            consensus_commit != '' AND cl_branch_commit_count = 1, 'commit_untagged',
            consensus_commit != '' AND cl_branch_commit_count > 1, 'commit_ambiguous',
            consensus_commit != '', 'commit_unmatched',
            graffiti_version != '' AND cl_version_release IS NOT NULL, 'version_match',
            graffiti_version != '', 'version_unmatched',
            graffiti_format = 'codes_min', 'codes_min',
            'no_commit'
        ) AS consensus_status,
        multiIf(
            execution_client = 'unknown', 'none',
            length(execution_commit) BETWEEN 1 AND 3, 'commit_short',
            match(execution_commit, '^0+$'), 'commit_zero',
            execution_commit != '' AND (el_commit_count = 1 OR el_published_commit_count = 1), 'commit_match',
            execution_commit != '' AND el_commit_count > 1, 'commit_ambiguous',
            execution_commit != '' AND el_branch_commit_count = 1, 'commit_untagged',
            execution_commit != '' AND el_branch_commit_count > 1, 'commit_ambiguous',
            execution_commit != '', 'commit_unmatched',
            graffiti_format = 'codes_min', 'codes_min',
            'no_commit'
        ) AS execution_status
    FROM resolved
)
SETTINGS join_use_nulls = 1
