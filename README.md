# xatu-cbt

This repo contains the clickhouse migrations and the models for [CBT](https://github.com/ethpandaops/cbt) on [xatu](https://github.com/ethpandaops/xatu) data.

## Clickhouse Migrations

> **NOTE:** before running migrations for the first time, make sure to run the [Initial setup](#initial-setup-before-migrations).

Use the [golange-migrate](https://github.com/golang-migrate/migrate) tool for simple manual file based migrations. Remember to follow the [Best practices: How to write migrations](https://github.com/golang-migrate/migrate/blob/master/MIGRATIONS.md) when adding new migrations.

```bash
migrate -database "clickhouse://127.0.0.1:9000?username=admin&password=XXX_PASSWORD_XXX&database=default&x-multi-statement=true&x-cluster-name='{cluster}'&x-migrations-table=schema_migrations_cbt&x-migrations-table-engine=ReplicatedMergeTree" -path migrations up
```

### Initial setup before migrations

`golang-migrate` doesn't support the distrubted tables in clickhouse so we can do some setup to get it working. This way we can use `golang-migrate` against any of the clickhouse nodes.

Create the replication table;
```sql
CREATE TABLE default.schema_migrations_cbt_local ON CLUSTER '{cluster}'
(
    `version` Int64,
    `dirty` UInt8,
    `sequence` UInt64
) Engine = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
ORDER BY sequence
```

Create the distributed table;
```sql
CREATE TABLE schema_migrations_cbt on cluster '{cluster}' AS schema_migrations_cbt_local
ENGINE = Distributed('{cluster}', default, schema_migrations_cbt_local, cityHash64(`version`));
```

### Run locally

- run [xatu locally via docker compose](https://github.com/ethpandaops/xatu?tab=readme-ov-file#locally-via-docker-compose)
- run `docker compose up -d` in this directory
- import data from [xatu-data](https://github.com/ethpandaops/xatu-data?tab=readme-ov-file#working-with-the-data)
