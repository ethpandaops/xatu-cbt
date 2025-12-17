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

# Set default goal explicitly (before catch-all rules interfere)
.DEFAULT_GOAL := all

# Docker image for CBT
CBT_DOCKER_IMAGE=ethpandaops/cbt:debian-latest

# Check for Docker image updates (once per day)
.docker-check-timestamp:
	@mkdir -p .make-cache
	@if [ ! -f .make-cache/docker-pull-timestamp ] || \
		[ $$(( $$(date +%s) - $$(stat -c %Y .make-cache/docker-pull-timestamp 2>/dev/null || echo 0) )) -gt 86400 ]; then \
		echo "Checking for CBT Docker image updates..."; \
		docker pull $(CBT_DOCKER_IMAGE) && \
		touch .make-cache/docker-pull-timestamp; \
	else \
		echo "Docker image checked recently (within 24h), skipping..."; \
	fi

.PHONY: docker-check
docker-check: .docker-check-timestamp

# Default target - build and run interactive
.PHONY: all
all: docker-check build run

# Run the binary in interactive mode (TUI)
.PHONY: run
run: build
	$(BINARY_PATH)

# Build the binary
.PHONY: build
build: #docker-check
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
	@# Load .env file and check NETWORK is set
	@if [ -f .env ]; then \
		export $$(grep -v '^#' .env | grep -v '^$$' | sed 's/#.*//' | xargs); \
	fi; \
	if [ -z "$$NETWORK" ]; then \
		echo "Error: NETWORK environment variable is not set"; \
		echo "Please set NETWORK in your .env file"; \
		exit 1; \
	fi; \
	echo "Using network: $$NETWORK"; \
	echo "Setting up ClickHouse infrastructure first..."; \
	if [ "$$XATU_SOURCE" = "external" ] && [ -n "$$XATU_URL" ]; then \
		echo "Using external xatu source: $$XATU_URL"; \
		go run $(MAIN_PATH) infra start --xatu-source external --xatu-url "$$XATU_URL"; \
	else \
		go run $(MAIN_PATH) infra start; \
	fi; \
	echo "Setting up network database..."; \
	go run $(MAIN_PATH) network setup -f
	@echo "Pulling clickhouse-proto-gen image..."
	@docker pull ethpandaops/clickhouse-proto-gen:latest
	@echo "Removing existing protobuf files..."
	@rm -rf pkg/proto/clickhouse
	@echo "Generating protobuf files from ClickHouse tables..."
	@if [ -f .env ]; then export $$(grep -v '^#' .env | grep -v '^$$' | sed 's/#.*//' | xargs); fi; \
	if [ -z "$$NETWORK" ]; then \
		echo "Error: NETWORK environment variable is not set"; \
		exit 1; \
	fi; \
	TABLES=$$(ls models/transformations/*.{sql,yml,yaml} 2>/dev/null | xargs -n1 basename | sed -E 's/\.(sql|yml|yaml)$$//' | tr '\n' ',' | sed 's/,$$//'); \
	USER=$${CLICKHOUSE_USERNAME:-default}; \
	PASS=$${CLICKHOUSE_PASSWORD:-supersecret}; \
	docker run --rm -v "$$(pwd):/workspace" \
		--user "$$(id -u):$$(id -g)" \
		--network xatu_xatu-net \
		ethpandaops/clickhouse-proto-gen \
		--dsn "clickhouse://$$USER:$$PASS@xatu-cbt-clickhouse-01:9000/$$NETWORK" \
		--tables "admin_incremental,$$TABLES" \
		--out /workspace/pkg/proto/clickhouse \
		--package cbt \
		--go-package github.com/ethpandaops/xatu-cbt/pkg/proto/clickhouse \
		--include-comments \
		--enable-api \
		--api-table-prefixes "fct" \
		--api-base-path "/api/v1" \
		--bigint-to-string "*.consensus_payload_value,*.execution_payload_value"
	@echo "Updating buf dependencies..."
	@buf dep update
	@echo "Generating Go protobuf code..."
	@buf generate

# Docker compose commands
# Usage: make docker compose up mainnet
.PHONY: docker
docker:
	@if [ "$(word 2,$(MAKECMDGOALS))" = "compose" ] && [ "$(word 3,$(MAKECMDGOALS))" = "up" ] && [ -n "$(word 4,$(MAKECMDGOALS))" ]; then \
		NETWORK=$(word 4,$(MAKECMDGOALS)); \
		if [ ! -f ".env.$$NETWORK" ]; then \
			echo "Error: .env.$$NETWORK file does not exist"; \
			echo "Please create .env.$$NETWORK before running this command"; \
			exit 1; \
		fi; \
		echo "Starting docker compose for network: $$NETWORK"; \
		$(MAKE) build; \
		$(BINARY_PATH) network setup -f --env .env.$$NETWORK; \
		docker compose --env-file .env.$$NETWORK -p cbt-$$NETWORK up -d; \
	elif [ "$(word 2,$(MAKECMDGOALS))" = "compose" ] && [ "$(word 3,$(MAKECMDGOALS))" = "down" ] && [ -n "$(word 4,$(MAKECMDGOALS))" ]; then \
		NETWORK=$(word 4,$(MAKECMDGOALS)); \
		if [ ! -f ".env.$$NETWORK" ]; then \
			echo "Error: .env.$$NETWORK file does not exist"; \
			echo "Please create .env.$$NETWORK before running this command"; \
			exit 1; \
		fi; \
		echo "Stopping docker compose for network: $$NETWORK"; \
		docker compose --env-file .env.$$NETWORK -p cbt-$$NETWORK down -v; \
	else \
		echo "Usage: make docker compose up <network> OR make docker compose down <network>"; \
		exit 1; \
	fi

# Catch-all targets for docker compose commands
compose:
	@:
up:
	@:
down:
	@:
%:
	@:

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
	@echo "  make docker compose up <network>   - Start CBT for specified network"
	@echo "  make docker compose down <network> - Stop CBT for specified network"
	@echo "  make help    - Show this help message"
	@echo ""
	@echo "Usage:"
	@echo "  ./bin/xatu-cbt              # Launch interactive TUI"
	@echo "  ./bin/xatu-cbt show-config  # Show config via CLI"
	@echo "  ./bin/xatu-cbt --help       # Show CLI help"
	@echo ""
	@echo "Docker Compose examples:"
	@echo "  make docker compose up mainnet"
	@echo "  make docker compose down mainnet"
