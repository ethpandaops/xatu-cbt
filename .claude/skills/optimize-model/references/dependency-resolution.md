# Dependency Resolution Rules

Use these rules to convert CBT transformation model templates into a runnable read-only SQL query.

## Scope
- Input: one file in `models/transformations/*.sql`
- Output: resolved SQL for analysis and benchmarking only (no write back to model files)

## Dependency Classification
1. Parse `dependencies:` entries in frontmatter.
2. Parse helper references in body:
   - `{{ index .dep "{{external}}" "table" "helpers" "from" }}`
   - `{{ index .dep "{{transformation}}" "table" "helpers" "from" }}`
3. Confirm by repo existence:
   - external: `models/external/<table>.sql`
   - transformation: `models/transformations/<table>.sql`
4. If still ambiguous, ask user to classify before benchmarking.

## Substitution
- External dependency replacement uses configurable template:
  - Default: `cluster('{remote_cluster}', database.table_name)`
  - Replace `database` and `table_name` with confirmed external database and dependency table.
  - If template uses `cluster(...)`, resolve dependency table to `<table>_local` (not the Distributed table name).
- Transformation dependency replacement is direct table access:
  - `` `<transformation_database>`.`<table>` ``

## Common CBT Variables
For benchmark rendering, substitute these defaults unless user overrides:
- `{{ .env.NETWORK }}` -> `mainnet`
- `{{ .bounds.start }}` / `{{ .bounds.end }}` -> benchmark window unix timestamps
- `{{ .task.start }}` -> benchmark run timestamp

If unresolved `{{ ... }}` fragments remain, stop and ask targeted follow-up questions.

## Read-Only Conversion
Transformation models are `INSERT INTO ... WITH/SELECT ...`.
For benchmarking, strip `INSERT INTO` and run only the read query.

## Safety
- Never edit source model SQL.
- Never run DDL/DML during optimization workflow.
- Always return resolved SQL plus unresolved fragments (if any).
