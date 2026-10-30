---
table: canonical_beacon_block_reward
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: slot
lag: 1920
---
SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
    {{ else }}
      -- Block rewards before Altair (phase0) are in their own table. They are
      -- included once phase0 reaches the epoch before this table's first epoch
      -- (the Altair fork). Until then the range starts at this table's first block.
      toUnixTimestamp(if(
        (
          SELECT max(epoch_start_date_time) FROM {{ .self.helpers.from | replace (printf "`%s" .self.table) "`canonical_beacon_phase0_block_reward" }}
          WHERE meta_network_name = '{{ .env.NETWORK }}'
        ) >= min(epoch_start_date_time) - INTERVAL 384 SECOND,
        (
          SELECT min(slot_start_date_time) FROM {{ .self.helpers.from | replace (printf "`%s" .self.table) "`canonical_beacon_phase0_block_reward" }}
          WHERE meta_network_name = '{{ .env.NETWORK }}'
            AND slot_start_date_time >= fromUnixTimestamp({{ default "0" .env.EXTERNAL_MODEL_MIN_TIMESTAMP }})
        ),
        min(slot_start_date_time)
      )) as min,
    {{ end }}
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM {{ .self.helpers.from }}
WHERE
    meta_network_name = '{{ .env.NETWORK }}'

    -- previous_max if incremental scan and is set, otherwise default/env
    {{- $ts := default "0" .env.EXTERNAL_MODEL_MIN_TIMESTAMP -}}
    {{- if .cache.is_incremental_scan -}}
      {{- if .cache.previous_max -}}
        {{- $ts = .cache.previous_max -}}
      {{- end -}}
    {{- end }}
    AND slot_start_date_time >= fromUnixTimestamp({{ $ts }})
    {{- if .cache.is_incremental_scan }}
      AND slot_start_date_time <= fromUnixTimestamp({{ $ts }}) + {{ default "100000" .env.EXTERNAL_MODEL_SCAN_SIZE_TIMESTAMP }}
    {{- end }}

