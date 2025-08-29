package actions

import (
	"context"
	"fmt"
	"strings"

	"github.com/savid/xatu-cbt/pkg/clickhouse"
	"github.com/savid/xatu-cbt/pkg/config"
)

// Teardown validates config and drops the ClickHouse database for the configured network
func Teardown(isInteractive, skipConfirm bool) error { //nolint:gocyclo // Complex logic for database cleanup
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
		if closeErr := conn.Close(); closeErr != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", closeErr)
		}
	}()

	// Check if database exists
	var dbExists uint64
	checkQuery := fmt.Sprintf("SELECT count() FROM system.databases WHERE name = '%s'", cfg.Network)
	if checkErr := conn.QueryRow(context.Background(), checkQuery).Scan(&dbExists); checkErr != nil {
		return fmt.Errorf("failed to check if database exists: %w", checkErr)
	}

	if dbExists == 0 {
		fmt.Printf("ℹ️  Database '%s' does not exist, nothing to teardown\n", cfg.Network)
		fmt.Println("\n✅ Teardown completed successfully!")
		return nil
	}

	// Instead of dropping the database, truncate all _local tables except schema_migrations_local
	// This preserves the database structure and avoids ZooKeeper replica conflicts
	fmt.Printf("\n🗑️  Cleaning data from database '%s'...\n", cfg.Network)

	// Get all _local tables in the database
	tablesQuery := fmt.Sprintf(`
		SELECT name 
		FROM system.tables 
		WHERE database = '%s' 
		AND name LIKE '%%_local' 
		AND name != 'schema_migrations_local'
		ORDER BY name
	`, cfg.Network)

	rows, err := conn.Query(context.Background(), tablesQuery)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			fmt.Printf("Warning: failed to close rows: %v\n", closeErr)
		}
	}()

	tables := make([]string, 0, 50) // Pre-allocate for typical number of tables
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if len(tables) == 0 { //nolint:nestif // Clear logic for handling table truncation
		fmt.Printf("ℹ️  No data tables found in database '%s'\n", cfg.Network)
	} else {
		fmt.Printf("📊 Found %d tables to clean\n", len(tables))

		// Truncate each table
		for _, table := range tables {
			var truncateQuery string
			if cfg.ClickhouseCluster != "" && cfg.ClickhouseCluster != "{cluster}" {
				truncateQuery = fmt.Sprintf("TRUNCATE TABLE `%s`.`%s` ON CLUSTER '%s'",
					cfg.Network, table, cfg.ClickhouseCluster)
			} else {
				truncateQuery = fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", cfg.Network, table)
			}

			fmt.Printf("  🗑️  Truncating %s...\n", table)
			if err := conn.Exec(context.Background(), truncateQuery); err != nil {
				// Log warning but continue with other tables
				fmt.Printf("  ⚠️  Warning: failed to truncate %s: %v\n", table, err)
			}
		}
	}

	fmt.Printf("✅ Database '%s' has been cleaned!\n", cfg.Network)

	fmt.Println("\n✅ Teardown completed successfully!")
	return nil
}
