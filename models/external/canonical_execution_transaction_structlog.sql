---
table: canonical_execution_transaction_structlog
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: block
lag: 64
---
-- This table is very large, so bounds queries are slow (~15s).
-- Using ORDER BY LIMIT 1 with generous timeouts.
SELECT
    {{ if .cache.is_incremental_scan }}
      toUInt64('{{ .cache.previous_min }}') as min,
    {{ else }}
      (
        SELECT block_number
        FROM {{ .self.helpers.from }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
        ORDER BY block_number ASC
        LIMIT 1
      ) as min,
    {{ end }}
    (
        SELECT block_number
        FROM {{ .self.helpers.from }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          {{- $bn := default "0" .env.EXTERNAL_MODEL_MIN_BLOCK -}}
          {{- if .cache.is_incremental_scan -}}
            {{- if .cache.previous_max -}}
              {{- $bn = .cache.previous_max -}}
            {{- end -}}
          {{- end }}
          AND block_number >= {{ $bn }}
          {{- if .cache.is_incremental_scan }}
            AND block_number <= {{ $bn }} + {{ default "10000" .env.EXTERNAL_MODEL_SCAN_SIZE_BLOCK }}
          {{- end }}
        ORDER BY block_number DESC
        LIMIT 1
    ) as max
SETTINGS
    connect_timeout_with_failover_ms = 60000,
    receive_timeout = 120,
    send_timeout = 120
