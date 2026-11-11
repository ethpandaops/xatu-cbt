---
table: fct_block_graffiti_head
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 12s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - graffiti
  - client
  - head
dependencies:
  - "{{transformation}}.fct_block_head"
  - "{{transformation}}.dim_node"
  # The sentry graffiti covers the newest blocks, the cannon graffiti the older ones written before sentries recorded
  # it. Either is enough, and the head bounds the range.
  - - "{{external}}.beacon_api_eth_v2_beacon_block"
    - "{{external}}.canonical_beacon_block_graffiti"
---
-- Graffiti and the proposer clients parsed from it for every block of the unfinalized chain, so the newest slots have
-- clients before fct_block_graffiti has them at finality. Blocks with no graffiti from either source get no row.
-- The parsing is fct_block_graffiti's, and the two must change together.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Blocks of the unfinalized chain, as fct_block_head accepted them
head_blocks AS (
    SELECT slot, slot_start_date_time, epoch, epoch_start_date_time, toString(block_root) AS block_root, proposer_index
    FROM {{ index .dep "{{transformation}}" "fct_block_head" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
-- Sentries record graffiti since xatu v1.30.11. Rows written before it carry NULL.
sentry_graffiti AS (
    SELECT slot_start_date_time, toString(block_root) AS root, any(graffiti_text) AS text
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v2_beacon_block" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND graffiti_text IS NOT NULL
    GROUP BY slot_start_date_time, root
),
-- Cannon writes graffiti once a block is finalized, which covers the rows sentries wrote before recording it
cannon_graffiti AS (
    SELECT slot_start_date_time, toString(block_root) AS root, any(graffiti_text) AS text
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_graffiti" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY slot_start_date_time, root
),
head_graffiti AS (
    SELECT
        h.slot AS slot,
        h.slot_start_date_time AS slot_start_date_time,
        h.epoch AS epoch,
        h.epoch_start_date_time AS epoch_start_date_time,
        h.block_root AS block_root,
        h.proposer_index AS proposer_index,
        assumeNotNull(coalesce(s.text, c.text)) AS graffiti_text
    FROM head_blocks AS h
    GLOBAL LEFT JOIN sentry_graffiti AS s ON h.slot_start_date_time = s.slot_start_date_time AND h.block_root = s.root
    GLOBAL LEFT JOIN cannon_graffiti AS c ON h.slot_start_date_time = c.slot_start_date_time AND h.block_root = c.root
    WHERE coalesce(s.text, c.text) IS NOT NULL
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    proposer_index,
    graffiti_text,
    multiIf(
        codes_ok AND length(code_commits[1]) = 8, 'codes_full',
        codes_ok AND length(code_commits[1]) = 4, 'codes_long',
        codes_ok AND length(code_commits[1]) = 2, 'codes_short',
        codes_ok, 'codes_min',
        version_client != '', 'version_string',
        rp_ok, 'rocketpool',
        trimmed = '', 'empty',
        'none'
    ) AS graffiti_format,
    multiIf(
        cl_index > 0, cl_names[indexOf(cl_codes, codes[cl_index])],
        version_client != '', version_client,
        rp_ok, rp_cl_names[indexOf(rp_cl_codes, rp_codes[2])],
        'unknown'
    ) AS consensus_client,
    if(consensus_client = version_client, version, '') AS consensus_client_version,
    multiIf(
        cl_index > 0, code_commits[cl_index],
        consensus_client = version_client, version_commit,
        ''
    ) AS consensus_client_commit,
    multiIf(
        el_index > 0, el_names[indexOf(el_codes, codes[el_index])],
        rp_ok AND has(rp_el_codes, rp_codes[1]), rp_el_names[indexOf(rp_el_codes, rp_codes[1])],
        'unknown'
    ) AS execution_client,
    if(el_index > 0, code_commits[el_index], '') AS execution_client_commit,
    ifNull(node.source, '') AS entity
FROM (
    SELECT
        *,
        -- A code pair with commits outranks the trailing-word forms.
        multiIf(pair_ok, [pair[1], pair[3]], tail_ok, tail_codes, []) AS codes,
        multiIf(pair_ok, [pair[2], pair[4]], tail_ok, tail_commits, []) AS code_commits,
        pair_ok OR tail_ok AS codes_ok,
        arrayFirstIndex(c -> has(el_codes, c), codes) AS el_index,
        arrayFirstIndex(c -> has(cl_codes, c), codes) AS cl_index,
        version_match[1] AS version_client,
        version_match[2] AS version,
        version_match[3] AS version_commit,
        NOT codes_ok AND version_client = '' AND length(rp_codes) = 2 AS rp_ok
    FROM (
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            epoch_start_date_time,
            block_root,
            proposer_index,
            graffiti_text,
            trimBoth(graffiti_text) AS trimmed,
            -- ClientCode list from execution-apis src/engine/identification.md
            ['BU', 'EJ', 'EG', 'EX', 'GE', 'NM', 'NE', 'TE', 'RH'] AS el_codes,
            ['besu', 'ethereumjs', 'erigon', 'ethrex', 'geth', 'nethermind', 'nimbusel', 'trin-execution', 'reth'] AS el_names,
            ['CN', 'LA', 'GR', 'LH', 'LS', 'NB', 'PM', 'TK'] AS cl_codes,
            ['caplin', 'ethlambda', 'grandine', 'lighthouse', 'lodestar', 'nimbus', 'prysm', 'teku'] AS cl_names,
            -- Code pair with commits ending a word, e.g. GEa579LH6db6, NMb3PM03, EGbaacCNbaac Erigon.
            arrayElement(extractAllGroups(trimmed, '(?P<code1>[A-Z]{2})(?P<commit1>[0-9a-f]{8}|[0-9a-f]{4}|[0-9a-f]{2})(?P<code2>[A-Z]{2})(?P<commit2>[0-9a-f]{8}|[0-9a-f]{4}|[0-9a-f]{2})(?:\\s|$)'), -1) AS pair,
            -- Last word: a code pair, one code or one code with a commit, e.g. GEPM, GE, TKf863.
            extractGroups(trimmed, '(?:^|[^A-Za-z0-9])(?P<code1>[A-Z]{2})(?P<rest>[A-Z]{2}|[0-9a-f]{8}|[0-9a-f]{4}|[0-9a-f]{2})?$') AS tail,
            if(match(tail[2], '^[A-Z]{2}$'), [tail[1], tail[2]], [tail[1]]) AS tail_codes,
            if(match(tail[2], '^[A-Z]{2}$'), ['', ''], [tail[2]]) AS tail_commits,
            -- EL code first, except teku v24.4 to v25.3 which put TK first.
            length(pair) = 4
                AND length(pair[2]) = length(pair[4])
                AND ((has(el_codes, pair[1]) AND has(cl_codes, pair[3])) OR (pair[1] = 'TK' AND has(el_codes, pair[3]))) AS pair_ok,
            -- Clients only attach a commit to a lone code when it is their own consensus code.
            length(tail) = 2 AND multiIf(
                length(tail_codes) = 2, (has(el_codes, tail[1]) AND has(cl_codes, tail[2])) OR (tail[1] = 'TK' AND has(el_codes, tail[2])),
                tail[2] != '', has(cl_codes, tail[1]),
                has(el_codes, tail[1]) OR has(cl_codes, tail[1])
            ) AS tail_ok,
            -- Client default graffiti as [client, version, commit].
            multiIf(
                match(trimmed, '^Lighthouse/v[0-9]'), ['lighthouse'] || extractGroups(trimmed, '^Lighthouse/(?P<version>v[0-9]\\S*?)(?:-(?P<commit>[0-9a-f]{7}))?\\+?$'),
                match(trimmed, '^Nimbus/v[0-9]'), ['nimbus'] || extractGroups(trimmed, '^Nimbus/(?P<version>v[0-9][^\\s-]*)-(?P<commit>[0-9a-f]+)(?:-\\S+)?$'),
                match(trimmed, '^teku/v[0-9]'), ['teku'] || extractGroups(trimmed, '^teku/(?P<version>v[0-9]\\S*)(?P<commit>)$'),
                match(trimmed, '^Lodestar-v[0-9]'), ['lodestar'] || extractGroups(trimmed, '^Lodestar-(?P<version>v[0-9][^/\\s]*)(?:.*/(?P<commit>[0-9a-f]{7}))?$'),
                match(trimmed, '(?:^|\\s)Grandine/[0-9]'), ['grandine'] || extractGroups(trimmed, '(?:^|\\s)Grandine/(?P<version>[0-9][^\\s-]*)(?:-(?P<commit>[0-9a-f]{7,8}))?$'),
                []
            ) AS version_parts,
            if(length(version_parts) = 3, version_parts, ['', '', '']) AS version_match,
            -- Rocket Pool smartnode default RP-<EL initial><CL initial> v<version>, X marks an external EL.
            extractGroups(trimmed, '^RP-(?P<el>[GNBREX])(?P<cl>[LSNPT]) v[0-9]') AS rp_codes,
            ['G', 'N', 'B', 'R', 'E'] AS rp_el_codes,
            ['geth', 'nethermind', 'besu', 'reth', 'erigon'] AS rp_el_names,
            ['L', 'S', 'N', 'P', 'T'] AS rp_cl_codes,
            ['lighthouse', 'lodestar', 'nimbus', 'prysm', 'teku'] AS rp_cl_names
        FROM head_graffiti
    )
) AS parsed
-- Entity is the current dim_node mapping of the proposer, not a historical snapshot.
LEFT JOIN (
    SELECT validator_index, source
    FROM {{ index .dep "{{transformation}}" "dim_node" "helpers" "from" }} FINAL
) AS node ON parsed.proposer_index = node.validator_index
SETTINGS join_use_nulls = 1
