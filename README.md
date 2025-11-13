# xatu-cbt

This repo contains the clickhouse migrations and the models for [CBT](https://github.com/ethpandaops/cbt) on [xatu](https://github.com/ethpandaops/xatu) data.

## Documentation

- **[NAMING_CONVENTIONS.md](./NAMING_CONVENTIONS.md)** - Model naming conventions and patterns

## Getting Started

Create a `.env` file:

```bash
cp example.env .env
```

Example `.env` file:

```bash
# Network configuration (REQUIRED - used for container suffixes)
NETWORK=mainnet

# ClickHouse configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_NATIVE_PORT=9000
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_CLUSTER={cluster}

# Xatu configuration
XATU_REF=master # what xatu repo ref to use for testing

# Logging configuration
LOG_LEVEL=debug # debug, info, warn, error
```

#### Using Custom Environment Files

Use the `--env` flag to load a different environment file:

```bash
# CLI mode
./bin/xatu-cbt --env .env.production show-config

# TUI mode
./bin/xatu-cbt --env=.env.production
```

### Usage

#### Interactive TUI Mode

Simply run the binary without arguments to enter interactive mode:

```bash
# use the makefile to build and run the binary
make

# or manually
go build -o ./bin/xatu-cbt ./cmd/xatu-cbt
./bin/xatu-cbt
```

#### CLI Commands

##### Network Commands

These commands will setup cbt admin tables, go-migrate schemas tables and run the migrations relevant for the configured network. You can also teardown the network database.

> **Note:** this can be used against local clickhouse or a remote staging/production clickhouse.

```bash
# Setup network database (creates tables and migrations)
./bin/xatu-cbt network setup [--force]

# Teardown network database (truncates tables, preserves structure)
./bin/xatu-cbt network teardown [--force]
```

## Infrastructure Management

The platform provides a persistent ClickHouse cluster infrastructure shared between development and testing.

### Starting Infrastructure

Before running tests or development work, start the platform infrastructure:

```bash
# Start ClickHouse cluster, Zookeeper, and Redis
./bin/xatu-cbt infra start
```

This starts:
- 2-node ClickHouse cluster with Zookeeper coordination
- Redis for state management
- Shared across both ephemeral test databases and persistent network databases

### Managing Infrastructure

```bash
# Check status
./bin/xatu-cbt infra status

# Detailed status with container info
./bin/xatu-cbt infra status --verbose

# Stop infrastructure (cleans up test databases)
./bin/xatu-cbt infra stop

# Stop without cleaning up test databases
./bin/xatu-cbt infra stop --cleanup-test-dbs=false

# Reset completely (removes all volumes)
./bin/xatu-cbt infra reset
```

The `infra reset` command is useful when:
- Testing fresh xatu migrations
- Disk space is low
- You need a complete clean slate

## Running Tests

### Prerequisites

1. Start infrastructure: `./bin/xatu-cbt infra start`
2. Ensure `.env` file is configured (see Getting Started)

### Test Execution

```bash
# Run tests for a specific spec/network combination
./bin/xatu-cbt test spec --spec pectra --network mainnet

# Run with verbose output
./bin/xatu-cbt test spec --spec pectra --network mainnet --verbose

# Run fusaka tests on sepolia
./bin/xatu-cbt test spec --spec fusaka --network sepolia
```

Tests use ephemeral databases (e.g., `test_mainnet_pectra_*`) that are automatically managed.

### Test Structure

Tests are organized by network and spec:

```
tests/
├── mainnet/
│   └── pectra/
│       └── models/                # Model definitions with embedded config
│           ├── canonical_beacon_block.yaml
│           └── fct_block.yaml
└── sepolia/
    └── fusaka/
        └── models/
            └── ...
```

Each model file contains:
- **data**: Parquet file paths and row count expectations
- **sql**: Transformation query (if transformation model)
- **assertions**: SQL queries to validate results

### CI/CD Integration

GitHub Actions automatically tests each spec/network combination:

```bash
# Workflow steps
cp example.env .env
mkdir -p .parquet_cache
./bin/xatu-cbt infra start
./bin/xatu-cbt test spec --spec pectra --network mainnet --verbose
./bin/xatu-cbt infra stop
```

### Protobuf Generation

When adding or modifying transformation models, you must generate corresponding protobuf files:

```bash
# Generate protobuf files for all transformation models
make proto
```

**When to run `make proto`:**

- After adding a new transformation model (`.sql` file in `models/transformations/`)
- After modifying the schema or columns of an existing transformation model
- Before committing changes to transformation models

The generated protobuf files in `pkg/proto/clickhouse/` are used by CBT for type safety and schema validation. Each transformation model must have corresponding `.proto`, `.go`, and `.pb.go` files.
