---
name: optimize-model
description: Deeply optimize a Xatu CBT transformation model query using live ClickHouse evidence. Use when performance analysis is requested for `models/transformations/*.sql`, including dependency rendering, schema introspection (`SHOW CREATE` with `_local` table checks), cache-aware benchmarking, and high-impact recommendations without editing model files.
compatibility: Requires Bash 3.2+, Python 3, jq, HTTP access to ClickHouse on port 8123, and internet access for web research.
---

# Optimize Model (Agent Skills Adapter)

Canonical workflow: `../../.claude/skills/optimize-model/SKILL.md`.

Shared resources: `./scripts/`, `./references/`.

Inherited guardrails: HTTP only on `8123` (never native `9000`/`clickhouse-client`), no `--help` probing in normal flow, perform version-aware web research early when feasible, include `Research Evidence` in final output, apply version-gated hash checks with multi-window baseline-vs-candidate comparison, account for the legacy `argMax + LowCardinality` hash caveat (`LowCardinality -> String` cast for legacy/manual hash checks), and never edit model files.
