# Xatu CBT Makefile

# Binary name
BINARY_NAME=xatu-cbt
BINARY_PATH=./bin/$(BINARY_NAME)

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Main package path
MAIN_PATH=./cmd/xatu-cbt

# Default target - build and run interactive
.PHONY: all
all: build run

# Run the binary in interactive mode (TUI)
.PHONY: run
run: build
	$(BINARY_PATH)

# Build the binary
.PHONY: build
build:
	$(GOBUILD) -o $(BINARY_PATH) $(MAIN_PATH)

# Clean build artifacts
.PHONY: clean
clean:
	$(GOCLEAN)
	rm -f $(BINARY_PATH)

# Run tests
.PHONY: test
test:
	$(GOTEST) -v ./...

# Download dependencies
.PHONY: deps
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Format code
.PHONY: fmt
fmt:
	go fmt ./...

# Generate protobuf files from ClickHouse tables
.PHONY: proto
proto:
	@echo "Setting up ClickHouse infrastructure first..."
	@go run $(MAIN_PATH) infra setup
	@echo "Generating protobuf files from ClickHouse tables..."
	@TABLES=$$(ls models/transformations/*.sql | xargs -n1 basename | sed 's/\.sql$$//' | tr '\n' ',' | sed 's/,$$//'); \
	HOST=$${CLICKHOUSE_HOST:-xatu-clickhouse-01}; \
	docker run --rm -v "$$(pwd):/workspace" \
		--user "$$(id -u):$$(id -g)" \
		--network xatu_xatu-net \
		ethpandaops/clickhouse-proto-gen \
		--dsn "clickhouse://xatu-clickhouse-01:9000/$${NETWORK:-mainnet}" \
		--tables "$$TABLES" \
		--out /workspace/pkg/proto/clickhouse \
		--package cbt \
		--go-package github.com/ethpandaops/xatu-cbt/pkg/proto/clickhouse \
		--include-comments
	@echo "Generating Go protobuf code..."
	buf generate

# Help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make         - Build and run interactive TUI (default)"
	@echo "  make build   - Build the binary"
	@echo "  make run     - Run the interactive TUI"
	@echo "  make clean   - Remove build artifacts"
	@echo "  make test    - Run tests"
	@echo "  make deps    - Download and tidy dependencies"
	@echo "  make fmt     - Format Go code"
	@echo "  make proto   - Generate protobuf files from ClickHouse tables"
	@echo "  make help    - Show this help message"
	@echo ""
	@echo "Usage:"
	@echo "  ./bin/xatu-cbt              # Launch interactive TUI"
	@echo "  ./bin/xatu-cbt show-config  # Show config via CLI"
	@echo "  ./bin/xatu-cbt --help       # Show CLI help"