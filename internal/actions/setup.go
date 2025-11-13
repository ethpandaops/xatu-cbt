// Package actions contains the core business logic for xatu-cbt operations
package actions

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ethpandaops/xatu-cbt/internal/clickhouse"
	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/migrations"
)

// Setup validates config and sets up ClickHouse database for the configured network
func Setup(isInteractive, skipConfirm bool) error {
	// Load and validate config
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Validate config
	if valErr := validateConfig(cfg); valErr != nil {
		return valErr
	}

	// Show target info
	fmt.Println("\n📋 Setup Configuration:")
	fmt.Println("======================")
	fmt.Printf("Network:         %s\n", cfg.Network)
	fmt.Printf("ClickHouse Host: %s:%d\n", cfg.ClickhouseHost, cfg.ClickhouseNativePort)
	fmt.Printf("Username:        %s\n", cfg.ClickhouseUsername)
	fmt.Printf("Database Name:   %s\n", cfg.Network)
	if cfg.ClickhouseCluster != "" {
		fmt.Printf("Cluster:         %s\n", cfg.ClickhouseCluster)
	} else {
		fmt.Printf("Cluster:         (single-node)\n")
	}
	fmt.Println()

	// Handle confirmation for both TUI and CLI modes
	if !skipConfirm {
		if isInteractive {
			fmt.Printf("⚠️  You are about to setup database for network: %s\n", strings.ToUpper(cfg.Network))
			fmt.Println("This will create the database if it doesn't exist.")
		}
		// Return here so the caller can handle confirmation
		return nil
	}

	// Test connection
	fmt.Println("🔌 Testing ClickHouse connection...")
	if testErr := clickhouse.TestConnection(cfg); testErr != nil {
		return fmt.Errorf("connection test failed: %w", testErr)
	}
	fmt.Println("✅ Connection successful!")

	// Connect to ClickHouse
	fmt.Println("\n🔗 Connecting to ClickHouse...")
	conn, err := clickhouse.Connect(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
	}()

	// Create database
	fmt.Printf("\n📦 Creating database '%s' if it doesn't exist...\n", cfg.Network)
	if err := clickhouse.CreateDatabase(conn, cfg.Network, cfg.ClickhouseCluster); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	fmt.Printf("✅ Database '%s' is ready!\n", cfg.Network)

	// Create migration tables
	fmt.Printf("\n📊 Creating migration tables...\n")
	if err := clickhouse.CreateMigrationTables(conn, cfg.Network, cfg.ClickhouseCluster); err != nil {
		return fmt.Errorf("failed to create migration tables: %w", err)
	}
	fmt.Println("✅ Migration tables created successfully!")

	// Run migrations
	fmt.Printf("\n🔄 Running database migrations...\n")
	if err := migrations.PrepareAndRun(cfg); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	fmt.Println("\n🎉 Setup completed successfully!")
	return nil
}

var (
	// ErrNetworkNotSet is returned when the network is not configured
	ErrNetworkNotSet = errors.New("network is not set in configuration")
	// ErrNetworkIsDefault is returned when the network is set to 'default'
	ErrNetworkIsDefault = errors.New("network cannot be 'default' - please specify a valid network name (e.g., mainnet, holesky, sepolia)")
	// ErrHostNotSet is returned when the ClickHouse host is not configured
	ErrHostNotSet = errors.New("ClickHouse host is not set")
	// ErrPortNotSet is returned when the ClickHouse port is not configured
	ErrPortNotSet = errors.New("ClickHouse native port is not set")
	// ErrUsernameNotSet is returned when the ClickHouse username is not configured
	ErrUsernameNotSet = errors.New("ClickHouse username is not set")
)

// validateConfig checks if the configuration is valid for setup
func validateConfig(cfg *config.AppConfig) error {
	// Check network
	if cfg.Network == "" {
		return ErrNetworkNotSet
	}
	if cfg.Network == "default" {
		return ErrNetworkIsDefault
	}

	// Check ClickHouse host
	if cfg.ClickhouseHost == "" {
		return ErrHostNotSet
	}

	// Check ClickHouse native port
	if cfg.ClickhouseNativePort == 0 {
		return ErrPortNotSet
	}

	// Check username
	if cfg.ClickhouseUsername == "" {
		return ErrUsernameNotSet
	}

	return nil
}
