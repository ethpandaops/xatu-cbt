# xatu-cbt

This repo contains the clickhouse migrations and the models for [CBT](https://github.com/ethpandaops/cbt) on [xatu](https://github.com/ethpandaops/xatu) data.

## Getting Started

Create a `.env` file:

```bash
cp example.env .env
```

Example `.env` file:
```bash
# Network configuration
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

##### Test Commands

```bash
# Run a specific test suite
./bin/xatu-cbt test <test-name> [--skip-setup]

# Teardown test environment (cleanup containers and data)
./bin/xatu-cbt test teardown
```

### Creating and Running Tests

Tests are organized in the `tests/` directory with the following structure:
```
tests/
├── pectra/
│   ├── data/                      # Parquet data configuration
│   │   └── canonical_beacon_block.yaml
│   └── assertions/                # SQL assertions
│       └── canonical_beacon_block.yaml
```

Example test run:
```bash
# Run pectra test with full setup
./bin/xatu-cbt test pectra

# Run pectra test, skip xatu setup if already running
./bin/xatu-cbt test pectra --skip-setup
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
