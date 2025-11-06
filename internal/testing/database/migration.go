package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/sirupsen/logrus"
)

// MigrationRunner executes database migrations using golang-migrate.
// It supports templating of migration files to replace database placeholders.
// with actual database names, enabling migration reuse across multiple databases.
type MigrationRunner interface {
	// RunMigrations executes all pending migrations for the specified database.
	// The migrations are templated to replace ${NETWORK_NAME} and hardcoded
	// database references with the actual dbName parameter.
	RunMigrations(ctx context.Context, conn *sql.DB, dbName string) error
}

// migrationRunner implements MigrationRunner with support for SQL templating
type migrationRunner struct {
	migrationDir string             // Path to directory containing .sql migration files
	tablePrefix  string             // Prefix for migrations table name (e.g., "xatu_")
	log          logrus.FieldLogger // Logger for migration operations
}

// NewMigrationRunner creates a new migration runner with optional table name prefix.
func NewMigrationRunner(log logrus.FieldLogger, migrationDir, tablePrefix string) MigrationRunner {
	return &migrationRunner{
		migrationDir: migrationDir,
		tablePrefix:  tablePrefix,
		log:          log.WithField("component", "migration_runner"),
	}
}

// RunMigrations executes all pending migrations for a database.
//
// The method performs the following steps:
//  1. Templates migration files to replace ${NETWORK_NAME} with dbName
//  2. Creates an in-memory filesystem with templated SQL
//  3. Configures golang-migrate with per-database migrations table
//  4. Executes pending migrations while respecting context cancellation
func (r *migrationRunner) RunMigrations(ctx context.Context, conn *sql.DB, dbName string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Create templated filesystem.
	sourceFS, err := r.createTemplatedFS(dbName)
	if err != nil {
		return fmt.Errorf("creating templated migrations: %w", err)
	}

	r.log.WithField("database", dbName).Debug("running migrations, please wait")

	// Create iofs source driver.
	sourceDriver, err := iofs.New(sourceFS, ".")
	if err != nil {
		return fmt.Errorf("creating source driver: %w", err)
	}

	// Create ClickHouse database driver.
	migrationsTableName := fmt.Sprintf("%sschema_migrations_%s", r.tablePrefix, dbName)
	dbDriver, err := clickhouse.WithInstance(conn, &clickhouse.Config{
		DatabaseName:          dbName,
		MigrationsTable:       migrationsTableName, // Per-database migrations table with prefix
		MultiStatementEnabled: true,                // Enable multi-statement support for migration files
		MultiStatementMaxSize: 1024 * 1024,         // 1MB max per statement
	})
	if err != nil {
		return fmt.Errorf("creating clickhouse driver: %w", err)
	}

	// Create migrate instance.
	m, err := migrate.NewWithInstance("iofs", sourceDriver, dbName, dbDriver)
	if err != nil {
		return fmt.Errorf("creating migrate instance: %w", err)
	}

	// Run migrations
	done := make(chan error, 1)
	go func() {
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			done <- fmt.Errorf("running migrations: %w", err)
			return
		}
		done <- nil
	}()

	// Respect context cancellation during migration.
	select {
	case <-ctx.Done():
		return fmt.Errorf("migration canceled: %w", ctx.Err())
	case err := <-done:
		if err != nil {
			return err
		}
	}

	r.log.WithField("database", dbName).Debug("migrations completed successfully")

	return nil
}

// createTemplatedFS creates an in-memory filesystem with templated migration files.
//
// This function performs the following operations:
//  1. Reads all .sql files from the migration directory
//  2. Templates each file to replace ${NETWORK_NAME} and hardcoded database references
//  3. Adds no-op statements to empty migrations to prevent ClickHouse errors
//  4. Returns an in-memory filesystem (memFS) containing all templated SQL
func (r *migrationRunner) createTemplatedFS(dbName string) (fs.FS, error) {
	r.log.WithField("database", dbName).Debug("creating in-memory migration filesystem")

	// Read all migration files from disk.
	entries, err := os.ReadDir(r.migrationDir)
	if err != nil {
		return nil, fmt.Errorf("reading migration directory: %w", err)
	}

	// Create map to store templated SQL content in memory.
	files := make(map[string]string)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasSuffix(filename, ".sql") {
			continue
		}

		// Read migration file.
		content, err := os.ReadFile(filepath.Join(r.migrationDir, filename)) //nolint:gosec // G304: Reading migration files from trusted directory
		if err != nil {
			return nil, fmt.Errorf("reading migration file %s: %w", filename, err)
		}

		// Template the SQL.
		templatedSQL := r.templateSQL(string(content), dbName)

		// If migration is empty after templating, add a no-op statement to avoid "Empty query" error.
		if templatedSQL == "" {
			r.log.WithField("file", filename).Debug("empty migration file, adding no-op statement")
			templatedSQL = "SELECT 1; -- No-op migration"
		}

		files[filename] = templatedSQL
	}

	r.log.WithField("files", len(files)).Debug("templated migration files")

	return &memFS{files: files}, nil
}

// templateSQL replaces database placeholders and references in SQL migration content.
//
// This function performs comprehensive SQL templating with the following transformations:
//  1. ${NETWORK_NAME} → dbName (explicit placeholder replacement)
//  2. Hardcoded database names (default, dbt, admin) → dbName in multiple contexts:
//     - Distributed engine definitions: ENGINE = Distributed('{cluster}', default, ...)
//     - Database qualifiers: default.table_name → dbName.table_name
//     - INSERT/SELECT statements: FROM default ... → FROM dbName ...
//  3. Special handling for 'tmp' database → {dbName}_tmp for test isolation
//  4. ReplicatedMergeTree ZooKeeper paths → unique paths per database
//  5. DDL statement qualification via qualifyCreateStatements()
//
// These transformations prevent ClickHouse from defaulting to.
// the 'default' database when ON CLUSTER is used, ensuring proper test isolation.
func (r *migrationRunner) templateSQL(sqlTemplate, dbName string) string {
	// If dbName is "default", don't template anything.
	// This allows xatu migrations to run in their native default database.
	if dbName == "default" {
		return sqlTemplate
	}

	// Step 1: Replace explicit ${NETWORK_NAME} placeholder
	templated := strings.ReplaceAll(sqlTemplate, "${NETWORK_NAME}", dbName)

	// Replace hardcoded database names in Distributed engine definitions
	// Pattern: ENGINE = Distributed('{cluster}', default, table_name, rand())
	// Pattern: ENGINE = Distributed('{cluster}', dbt, table_name, cityHash64(...))
	// Pattern: ENGINE = Distributed('{cluster}', admin, table_name, ...)
	hardcodedDBs := []string{"default", "dbt", "admin"}
	for _, db := range hardcodedDBs {
		templated = strings.ReplaceAll(templated, fmt.Sprintf(", %s,", db), fmt.Sprintf(", %s,", dbName))
		templated = strings.ReplaceAll(templated, fmt.Sprintf(",%s,", db), fmt.Sprintf(",%s,", dbName))
	}

	// Replace 'tmp' with test-specific tmp database to avoid conflicts between parallel tests
	// Example: tmp.staging_table → test_mainnet_pectra_123_tmp.staging_table
	tmpDB := fmt.Sprintf("%s_tmp", dbName)
	templated = strings.ReplaceAll(templated, ", tmp,", fmt.Sprintf(", %s,", tmpDB))
	templated = strings.ReplaceAll(templated, ",tmp,", fmt.Sprintf(",%s,", tmpDB))

	// Qualify CREATE/ALTER/DROP statements with database name
	// Pattern: DROP TABLE [IF EXISTS] table_name ... → DROP TABLE [IF EXISTS] dbName.table_name ...
	// Pattern: CREATE TABLE table_name ON CLUSTER ... → CREATE TABLE dbName.table_name ON CLUSTER ...
	// Pattern: CREATE MATERIALIZED VIEW view_name ON CLUSTER ... → CREATE MATERIALIZED VIEW dbName.view_name ON CLUSTER ...
	// Pattern: ALTER TABLE table_name ... → ALTER TABLE dbName.table_name ...
	// This prevents ClickHouse from defaulting to 'default' database when using ON CLUSTER
	templated = r.qualifyCreateStatements(templated, dbName)

	// Replace database qualifiers in INSERT/SELECT statements
	// Ensures all query references point to the correct test database
	// Example: FROM default.table → FROM test_mainnet_pectra_123.table
	for _, db := range hardcodedDBs {
		// Replace qualified table references
		templated = strings.ReplaceAll(templated, fmt.Sprintf("%s.", db), fmt.Sprintf("%s.", dbName))
		// Replace FROM/INTO clauses with database names
		templated = strings.ReplaceAll(templated, fmt.Sprintf("FROM %s ", db), fmt.Sprintf("FROM %s ", dbName))
		templated = strings.ReplaceAll(templated, fmt.Sprintf("INTO %s ", db), fmt.Sprintf("INTO %s ", dbName))
	}

	// Replace 'tmp' database references with test-specific tmp database
	templated = strings.ReplaceAll(templated, "tmp.", fmt.Sprintf("%s.", tmpDB))
	templated = strings.ReplaceAll(templated, "FROM tmp ", fmt.Sprintf("FROM %s ", tmpDB))
	templated = strings.ReplaceAll(templated, "INTO tmp ", fmt.Sprintf("INTO %s ", tmpDB))

	// Replace database names in ReplicatedMergeTree ZooKeeper paths
	// Pattern: '/clickhouse/{installation}/{cluster}/default/tables/{table}/{shard}'
	// These paths must be unique per test to avoid replica conflicts across parallel tests
	// Each test database needs its own ZooKeeper path to prevent replica coordination issues
	for _, db := range []string{"default", "dbt", "admin"} {
		// Replace /{cluster}/default/ → /{cluster}/test_mainnet_pectra_123/
		oldPath := fmt.Sprintf("/{cluster}/%s/", db)
		newPath := fmt.Sprintf("/{cluster}/%s/", dbName)
		templated = strings.ReplaceAll(templated, oldPath, newPath)

		// Replace /default/tables/ → /test_mainnet_pectra_123/tables/
		// Handles alternative ZooKeeper path structures
		oldPath2 := fmt.Sprintf("/%s/tables/", db)
		newPath2 := fmt.Sprintf("/%s/tables/", dbName)
		templated = strings.ReplaceAll(templated, oldPath2, newPath2)
	}

	// Handle 'tmp' separately - it should use the test-specific _tmp database
	// Example: /{cluster}/tmp/ → /{cluster}/test_mainnet_pectra_123_tmp/
	templated = strings.ReplaceAll(templated, "/{cluster}/tmp/", fmt.Sprintf("/{cluster}/%s/", tmpDB))
	templated = strings.ReplaceAll(templated, "/tmp/tables/", fmt.Sprintf("/%s/tables/", tmpDB))

	// Trim whitespace to avoid empty query errors.
	return strings.TrimSpace(templated)
}

// qualifyDDLStatement processes DROP, TRUNCATE, CREATE, and ALTER statements.
// Returns the qualified line and true if processing succeeded, original line and false otherwise.
//
//nolint:gocyclo // SQL parsing logic requires multiple branches for different DDL patterns
func qualifyDDLStatement(line, upperLine string, parts []string, dbName string) (string, bool) {
	var objNameIdx int

	switch {
	case strings.HasPrefix(upperLine, "DROP TABLE"):
		objNameIdx = 2 // DROP TABLE table_name
		// Check for "IF EXISTS" clause
		if len(parts) >= 5 && strings.EqualFold(parts[2], "IF") && strings.EqualFold(parts[3], "EXISTS") {
			objNameIdx = 4
		}
	case strings.HasPrefix(upperLine, "TRUNCATE TABLE"):
		objNameIdx = 2 // TRUNCATE TABLE table_name
	case strings.HasPrefix(upperLine, "CREATE TABLE"):
		objNameIdx = 2 // CREATE TABLE table_name
		// Check for "IF NOT EXISTS" clause
		if len(parts) >= 5 && strings.EqualFold(parts[2], "IF") &&
			strings.EqualFold(parts[3], "NOT") && strings.EqualFold(parts[4], "EXISTS") {
			objNameIdx = 5
		}
	case strings.HasPrefix(upperLine, "CREATE MATERIALIZED VIEW"):
		objNameIdx = 3 // CREATE MATERIALIZED VIEW view_name
	case strings.HasPrefix(upperLine, "ALTER TABLE"):
		objNameIdx = 2 // ALTER TABLE table_name
	default:
		return line, false
	}

	if len(parts) <= objNameIdx {
		return line, false
	}

	objName := parts[objNameIdx]
	if isQualified(objName, dbName) {
		return line, false
	}

	qualifiedName := qualifyName(objName, dbName)
	newLine := strings.Replace(line, objName, qualifiedName, 1)

	// For CREATE statements, also check for AS clause on same line
	if strings.HasPrefix(upperLine, "CREATE") && strings.Contains(strings.ToUpper(newLine), " AS ") {
		newLine = qualifyASClauseInline(newLine, dbName)
	}

	return newLine, true
}

// qualifyASClauseInline handles AS clauses within a line (used by CREATE TABLE ... AS).
func qualifyASClauseInline(line, dbName string) string {
	asIndex := strings.Index(strings.ToUpper(line), " AS ")
	if asIndex == -1 {
		return line
	}

	beforeAS := line[:asIndex+4]
	afterAS := strings.TrimSpace(line[asIndex+4:])
	asParts := strings.Fields(afterAS)

	if len(asParts) == 0 {
		return line
	}

	asTableName := asParts[0]
	if isQualified(asTableName, dbName) {
		return line
	}

	asQualifiedName := qualifyName(asTableName, dbName)
	afterAS = strings.Replace(afterAS, asTableName, asQualifiedName, 1)
	return beforeAS + afterAS
}

// qualifyClauseLine processes standalone AS, TO, and FROM clauses on their own lines.
// Returns the qualified line and true if processing succeeded, original line and false otherwise.
func qualifyClauseLine(line, upperLine string, parts []string, dbName string) (string, bool) {
	// Skip lines that look like column definitions (contain commas or data types)
	if strings.Contains(line, ",") || strings.Contains(upperLine, "FIXEDSTRING") || strings.Contains(upperLine, "CODEC") {
		return line, false
	}

	var tableName string

	switch {
	case strings.HasPrefix(upperLine, "AS ") && !strings.HasPrefix(upperLine, "AS SELECT"):
		// Standalone AS clause (not AS SELECT which is a query)
		if len(parts) < 2 {
			return line, false
		}
		tableName = parts[1]
	case strings.HasPrefix(upperLine, "TO "):
		// TO clause in materialized views
		if len(parts) < 2 {
			return line, false
		}
		tableName = parts[1]
	case strings.HasPrefix(upperLine, "FROM "):
		// FROM clause - skip if it's a function/subquery
		if len(parts) < 2 {
			return line, false
		}
		tableName = parts[1]
		if strings.HasPrefix(tableName, "(") {
			return line, false
		}
	default:
		return line, false
	}

	if isQualified(tableName, dbName) {
		return line, false
	}

	return strings.Replace(line, tableName, qualifyName(tableName, dbName), 1), true
}

// qualifyCreateStatements adds database qualifier to DDL statements
// Also qualifies unqualified table references in TO and FROM clauses.
// Handles patterns like:
//
//	DROP TABLE [IF EXISTS] table_name ... → DROP TABLE [IF EXISTS] dbName.table_name ...
//	TRUNCATE TABLE table_name ... → TRUNCATE TABLE dbName.table_name ...
//	CREATE TABLE table_name ON CLUSTER ... → CREATE TABLE dbName.table_name ON CLUSTER ...
//	CREATE MATERIALIZED VIEW view_name ... → CREATE MATERIALIZED VIEW dbName.view_name ...
//	ALTER TABLE table_name ON CLUSTER ... → ALTER TABLE dbName.table_name ON CLUSTER ...
//	TO table_name → TO dbName.table_name
//	FROM table_name → FROM dbName.table_name (if not already qualified)
func (r *migrationRunner) qualifyCreateStatements(sqlContent, dbName string) string {
	var (
		lines  = strings.Split(sqlContent, "\n")
		result = make([]string, 0, len(lines))
	)

	for _, line := range lines {
		var (
			trimmed   = strings.TrimSpace(line)
			upperLine = strings.ToUpper(trimmed)
			parts     = strings.Fields(trimmed)
		)

		// Try DDL statement qualification first (DROP, CREATE, ALTER, TRUNCATE)
		if qualifiedLine, handled := qualifyDDLStatement(line, upperLine, parts, dbName); handled {
			result = append(result, qualifiedLine)

			continue
		}

		// Try standalone clause qualification (AS, TO, FROM)
		if qualifiedLine, handled := qualifyClauseLine(line, upperLine, parts, dbName); handled {
			result = append(result, qualifiedLine)

			continue
		}

		// Keep line as-is if not matched above
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

// isQualified checks if a table name is already qualified with a database prefix.
func isQualified(name, dbName string) bool {
	return strings.Contains(name, ".") || strings.HasPrefix(name, "`"+dbName)
}

// qualifyName builds a fully qualified table name with backticks.
func qualifyName(name, dbName string) string {
	return fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(name, "`"))
}
