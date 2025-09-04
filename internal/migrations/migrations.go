// Package migrations handles database schema migrations
package migrations

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse" // clickhouse driver for migrations
	_ "github.com/golang-migrate/migrate/v4/source/file"         // file source driver for migrations
)

// PrepareAndRun prepares migration files by substituting network name and runs migrations
func PrepareAndRun(cfg *config.Config) error {
	// Create temp directory for processed migrations
	tempDir, err := os.MkdirTemp("", "xatu-cbt-migrations-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tempDir) // Clean up temp dir when done
	}()

	// Get the migrations directory (relative to the binary location)
	migrationsDir := "migrations"

	// Copy and process migration files
	if procErr := processMigrationFiles(migrationsDir, tempDir, cfg.Network); procErr != nil {
		return fmt.Errorf("failed to process migration files: %w", procErr)
	}

	// Build connection string for golang-migrate
	connStr := buildConnectionString(cfg)

	// Create migration instance
	m, err := migrate.New(
		fmt.Sprintf("file://%s", tempDir),
		connStr,
	)
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

// processMigrationFiles copies migration files from source to dest, substituting ${NETWORK_NAME}
func processMigrationFiles(sourceDir, destDir, network string) error {
	// Read all files from source directory
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only process .sql files
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		sourcePath := filepath.Join(sourceDir, entry.Name())
		destPath := filepath.Join(destDir, entry.Name())

		// Read source file
		// #nosec G304 -- sourcePath is constructed from known directory
		content, err := os.ReadFile(sourcePath)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", sourcePath, err)
		}

		// Replace ${NETWORK_NAME} with actual network
		processedContent := strings.ReplaceAll(string(content), "${NETWORK_NAME}", network)

		// Write processed content to destination
		if err := os.WriteFile(destPath, []byte(processedContent), 0o600); err != nil {
			return fmt.Errorf("failed to write file %s: %w", destPath, err)
		}
	}

	return nil
}

// buildConnectionString builds the ClickHouse connection string for golang-migrate
func buildConnectionString(cfg *config.Config) string {
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

// GetMigrationStatus returns the current migration version and dirty state
func GetMigrationStatus(cfg *config.Config) (version uint, dirty bool, err error) {
	// Create temp directory for processed migrations (needed for source)
	tempDir, err := os.MkdirTemp("", "xatu-cbt-migrations-status-*")
	if err != nil {
		return 0, false, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	// Copy migration files (needed for migrate instance)
	if procErr := processMigrationFiles("migrations", tempDir, cfg.Network); procErr != nil {
		return 0, false, fmt.Errorf("failed to process migration files: %w", procErr)
	}

	connStr := buildConnectionString(cfg)

	m, mErr := migrate.New(
		fmt.Sprintf("file://%s", tempDir),
		connStr,
	)
	if mErr != nil {
		return 0, false, fmt.Errorf("failed to create migration instance: %w", mErr)
	}
	defer func() {
		if _, closeErr := m.Close(); closeErr != nil {
			fmt.Printf("Warning: failed to close migration instance: %v\n", closeErr)
		}
	}()

	version, dirty, err = m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		// No migrations applied yet
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	return version, dirty, nil
}

// CopyMigrationsToDirectory copies and processes migration files to a specific directory
// This is useful for debugging or manual inspection
func CopyMigrationsToDirectory(sourceDir, destDir, network string) error {
	// Create destination directory if it doesn't exist
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	return processMigrationFiles(sourceDir, destDir, network)
}
