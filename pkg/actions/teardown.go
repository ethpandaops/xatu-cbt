package actions

import (
	"context"
	"fmt"
	"strings"

	"github.com/savid/xatu-cbt/pkg/clickhouse"
	"github.com/savid/xatu-cbt/pkg/config"
)

// Teardown validates config and drops the ClickHouse database for the configured network
func Teardown(isInteractive, skipConfirm bool) error {
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
	fmt.Println("\n⚠️  Teardown Configuration:")
	fmt.Println("========================")
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
			fmt.Printf("🗑️  You are about to DROP database: %s\n", strings.ToUpper(cfg.Network))
			fmt.Println("⚠️  WARNING: This will permanently delete ALL data in the database!")
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

	// Connect to ClickHouse (using default database)
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

	// Drop database
	fmt.Printf("\n🗑️  Dropping database '%s'...\n", cfg.Network)
	query := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", cfg.Network)
	if cfg.ClickhouseCluster != "" && cfg.ClickhouseCluster != "{cluster}" {
		query = fmt.Sprintf("DROP DATABASE IF EXISTS `%s` ON CLUSTER '%s'", cfg.Network, cfg.ClickhouseCluster)
	}

	if err := conn.Exec(context.Background(), query); err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	fmt.Printf("✅ Database '%s' has been dropped!\n", cfg.Network)

	fmt.Println("\n✅ Teardown completed successfully!")
	return nil
}
