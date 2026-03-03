# Xatu CBT Makefile

# Binary name
BINARY_NAME=xatu-cbt
BINARY_PATH=./bin/$(BINARY_NAME)

# Test parameters
NETWORK ?= mainnet
VERBOSE ?= --verbose

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
build: docker-check
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

# Smart model test runner
# Usage:
#   make test-models                          # changed models, mainnet
#   make test-models NETWORK=sepolia          # changed models, sepolia
#   make test-models ALL=1                    # all models, mainnet
#   make test-models MODELS=fct_block,fct_tx  # specific models
.PHONY: test-models
test-models: build
	@echo "Starting infrastructure..."
	@$(BINARY_PATH) infra start
	@echo ""
	@echo "\033[33m=== PARQUET CACHE ===\033[0m"
	@echo "\033[33mIf your test seed data has changed, you may need to clear stale parquet cache.\033[0m"
	@echo "\033[33mLocal:  make clear-parquet-cache\033[0m"
	@echo "\033[33mR2:     Purge via Cloudflare dashboard\033[0m"
	@echo ""
	@echo "Press Enter to continue or Ctrl+C to abort..."
	@read _dummy
	@if [ -n "$(ALL)" ]; then \
		echo "Running ALL model tests for $(NETWORK)..."; \
		$(BINARY_PATH) test all --network $(NETWORK) $(VERBOSE); \
	elif [ -n "$(MODELS)" ]; then \
		echo "Running tests for models: $(MODELS)..."; \
		$(BINARY_PATH) test models $(MODELS) --network $(NETWORK) $(VERBOSE); \
	else \
		echo "Detecting changed models..."; \
		IMPACTED=$$(./.github/scripts/detect_impacted_models.sh --network $(NETWORK)); \
		if [ "$$IMPACTED" = "all" ]; then \
			echo "Broad changes detected — running full test suite..."; \
			$(BINARY_PATH) test all --network $(NETWORK) $(VERBOSE); \
		elif [ "$$IMPACTED" = "none" ]; then \
			echo ""; \
			echo "\033[33m✓ No impacted models detected for $(NETWORK). Nothing to test.\033[0m"; \
			echo ""; \
			echo "  To run all tests:          make test-models ALL=1"; \
			echo "  To run specific models:    make test-models MODELS=fct_block,fct_tx"; \
			echo ""; \
		else \
			echo "Testing impacted models: $$IMPACTED"; \
			$(BINARY_PATH) test models $$IMPACTED --network $(NETWORK) $(VERBOSE); \
		fi; \
	fi

# Clear local parquet cache and warn about R2
.PHONY: clear-parquet-cache
clear-parquet-cache:
	@echo "Clearing local parquet cache (.parquet_cache/)..."
	@rm -rf .parquet_cache
	@echo ""
	@echo "=== WARNING ==="
	@echo "Local cache cleared. If you regenerated seed data, the Cloudflare R2"
	@echo "parquet cache may also be stale."
	@echo ""
	@echo "To purge R2 parquet cache:"
	@echo "  Purge via Cloudflare dashboard"
	@echo ""
	@echo "Press Enter to continue or Ctrl+C to abort..."
	@read _dummy

# Clear parquet cache then run test-models
.PHONY: test-models-clean
test-models-clean: clear-parquet-cache test-models

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
	@echo "  make test-models             - Test changed models (auto-detect via git diff)"
	@echo "  make test-models ALL=1       - Test all models"
	@echo "  make test-models MODELS=x,y  - Test specific models"
	@echo "  make clear-parquet-cache      - Clear local .parquet_cache/ (with R2 warning)"
	@echo "  make test-models-clean        - Clear parquet cache then run test-models"
	@echo "  make docker compose up <network>   - Start CBT for specified network"
	@echo "  make docker compose down <network> - Stop CBT for specified network"
	@echo "  make help    - Show this help message"
	@echo ""
	@echo "Test variables:"
	@echo "  NETWORK=mainnet  - Target network (default: mainnet)"
	@echo "  ALL=1            - Run all model tests"
	@echo "  MODELS=x,y       - Run specific model tests"
	@echo "  VERBOSE=         - Disable verbose output"
	@echo ""
	@echo "Usage:"
	@echo "  ./bin/xatu-cbt              # Launch interactive TUI"
	@echo "  ./bin/xatu-cbt show-config  # Show config via CLI"
	@echo "  ./bin/xatu-cbt --help       # Show CLI help"
	@echo ""
	@echo "Docker Compose examples:"
	@echo "  make docker compose up mainnet"
	@echo "  make docker compose down mainnet"
