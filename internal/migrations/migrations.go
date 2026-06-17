// Package migrations handles database schema migrations
package migrations

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse" // clickhouse driver for migrations
	_ "github.com/golang-migrate/migrate/v4/source/file"         // file source driver for migrations
)

// migrationsDirName is the directory holding the migration SQL, relative to the
// working directory (the repo root).
const migrationsDirName = "migrations"

// migrationSourceURL returns the golang-migrate file:// source URL for the
// migration set. The migrations are database-agnostic and applied verbatim — the
// target database is selected via the connection's database= parameter and
// resolved in-SQL by currentDatabase()/the {database} macro — so no per-network
// templating is needed.
func migrationSourceURL() (string, error) {
	absDir, err := filepath.Abs(migrationsDirName)
	if err != nil {
		return "", fmt.Errorf("failed to resolve migrations directory: %w", err)
	}

	return fmt.Sprintf("file://%s", absDir), nil
}

// PrepareAndRun runs the database-agnostic migrations against the network database.
func PrepareAndRun(cfg *config.AppConfig) error {
	source, err := migrationSourceURL()
	if err != nil {
		return err
	}

	// Build connection string for golang-migrate
	connStr := buildConnectionString(cfg)

	// Create migration instance
	m, err := migrate.New(source, connStr)
	if err != nil {
		return fmt.Errorf("failed to create migration instance: %w", err)
	}
	defer func() {
		if _, closeErr := m.Close(); closeErr != nil {
			fmt.Printf("Warning: failed to close migration instance: %v\n", closeErr)
		}
	}()

	// Run migrations up
	upErr := m.Up()
	if upErr != nil && !errors.Is(upErr, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run migrations: %w", upErr)
	}

	if upErr != nil && errors.Is(upErr, migrate.ErrNoChange) {
		fmt.Println("ℹ️  No new migrations to apply")
	} else {
		// Get the current version to show what was applied
		version, dirty, vErr := m.Version()
		if vErr != nil && !errors.Is(vErr, migrate.ErrNilVersion) {
			return fmt.Errorf("failed to get migration version: %w", vErr)
		}
		if !dirty {
			fmt.Printf("✅ Migrations applied successfully (current version: %d)\n", version)
		}
	}

	return nil
}

// buildConnectionString builds the ClickHouse connection string for golang-migrate
func buildConnectionString(cfg *config.AppConfig) string {
	// Build base connection string
	connStr := fmt.Sprintf("clickhouse://%s:%d?username=%s&database=%s&x-multi-statement=true",
		cfg.ClickhouseHost,
		cfg.ClickhouseNativePort,
		cfg.ClickhouseUsername,
		cfg.Network,
	)

	// Add password if set
	if cfg.ClickhousePassword != "" {
		connStr += fmt.Sprintf("&password=%s", cfg.ClickhousePassword)
	}

	// Add cluster configuration if set
	if cfg.ClickhouseCluster != "" {
		// For clustered setup, use ReplicatedMergeTree for migrations table
		connStr += fmt.Sprintf("&x-cluster-name=%s&x-migrations-table-engine=ReplicatedMergeTree", cfg.ClickhouseCluster)
	} else {
		// For single-node, use default TinyLog or MergeTree
		connStr += "&x-migrations-table-engine=MergeTree"
	}

	return connStr
}
