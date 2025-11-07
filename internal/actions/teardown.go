package actions

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ethpandaops/xatu-cbt/internal/clickhouse"
	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/infra"
	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

var (
	// ErrHostnameValidationFailed is returned when hostname validation fails for safety reasons.
	ErrHostnameValidationFailed = errors.New("hostname validation failed - operation blocked for safety")
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
			fmt.Printf("🗑️  You are about to TRUNCATE all tables in database: %s\n", strings.ToUpper(cfg.Network))
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

	// Validate hostname before allowing destructive operations
	fmt.Println("🔒 Validating hostname safety...")
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel) // Only show warnings and errors for teardown
	validator := infra.NewValidator(cfg.SafeHostnames, log)

	ctx := context.Background()
	err = validator.ValidateDriver(ctx, conn)
	if err != nil {
		fmt.Println() // Blank line before warning
		displayHostnameValidationError(err, cfg.SafeHostnames)
		return ErrHostnameValidationFailed
	}
	fmt.Println("✅ Hostname validated successfully!")

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

	tables := []string{}
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
			if cfg.ClickhouseCluster != "" {
				truncateQuery = fmt.Sprintf("TRUNCATE TABLE `%s`.`%s` ON CLUSTER '%s' SYNC SETTINGS alter_sync = 2, replication_wait_for_inactive_replica_timeout = 300, distributed_ddl_task_timeout = 300",
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

// displayHostnameValidationError displays a big red warning box when hostname validation fails
func displayHostnameValidationError(err error, whitelist []string) {
	red := color.New(color.FgRed, color.Bold).SprintFunc()

	// Extract hostname from error message if possible
	errMsg := err.Error()
	hostname := "UNKNOWN"
	if strings.Contains(errMsg, "host '") {
		parts := strings.Split(errMsg, "host '")
		if len(parts) > 1 {
			hostParts := strings.Split(parts[1], "'")
			if len(hostParts) > 0 {
				hostname = hostParts[0]
			}
		}
	}

	// Print header box, then content without borders
	fmt.Println(red("╔════════════════════════════════════════════════════════════════════╗"))
	fmt.Println(red("║         🚨  HOSTNAME VALIDATION FAILED  🚨                         ║"))
	fmt.Println(red("╚════════════════════════════════════════════════════════════════════╝"))
	fmt.Println(red(""))
	fmt.Println(red("  ⚠️  Non-whitelisted ClickHouse host detected!"))
	fmt.Println(red(""))
	fmt.Println(red("  Blocked Hostname: " + hostname))
	fmt.Println(red(""))
	fmt.Println(red("  Check for active port-forwards or SSH tunnels:"))
	fmt.Println(red("    • kubectl port-forward"))
	fmt.Println(red("    • ssh -L (local tunnels)"))
	fmt.Println(red("    • VPN/bastion connections"))
	fmt.Println(red(""))
	fmt.Println(red("  Current Whitelist: " + fmt.Sprintf("%v", whitelist)))
	fmt.Println(red(""))
	fmt.Println(red("  To allow, set environment variable:"))
	newWhitelist := append(append([]string{}, whitelist...), hostname)
	exportCmd := fmt.Sprintf("XATU_CBT_SAFE_HOSTS=%q", strings.Join(newWhitelist, ","))
	fmt.Println(red("    " + exportCmd))
	fmt.Println()
}
