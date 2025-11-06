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
//
//nolint:gocyclo // Complex SQL parsing for multiple ClickHouse statement types.
func (r *migrationRunner) qualifyCreateStatements(sqlContent, dbName string) string {
	lines := strings.Split(sqlContent, "\n")
	result := make([]string, 0, len(lines))

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upperLine := strings.ToUpper(trimmed)

		// Check if this line starts with DROP TABLE or TRUNCATE TABLE
		// Pattern: DROP TABLE [IF EXISTS] table_name [on cluster '{cluster}'] [SYNC];
		// Pattern: TRUNCATE TABLE table_name [on cluster '{cluster}'];
		if strings.HasPrefix(upperLine, "DROP TABLE") || strings.HasPrefix(upperLine, "TRUNCATE TABLE") { //nolint:nestif // SQL parsing logic - refactoring risky
			var (
				parts      = strings.Fields(trimmed)
				objNameIdx = 2 // Default: 0=DROP/TRUNCATE, 1=TABLE, 2=table_name
			)

			// Check for "IF EXISTS" clause (only for DROP).
			if strings.HasPrefix(upperLine, "DROP TABLE") && len(parts) >= 5 &&
				strings.EqualFold(parts[2], "IF") && strings.EqualFold(parts[3], "EXISTS") {
				objNameIdx = 4 // Skip to table name after "IF EXISTS".
			}

			if len(parts) > objNameIdx {
				objName := parts[objNameIdx]

				// Only qualify if table name doesn't already have database prefix.
				if !strings.Contains(objName, ".") && !strings.HasPrefix(objName, "`"+dbName) {
					// Replace the unqualified table name with qualified version.
					var (
						qualifiedName = fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(objName, "`"))
						newLine       = strings.Replace(line, objName, qualifiedName, 1)
					)

					result = append(result, newLine)

					continue
				}
			}
		}

		// Check if this line starts with CREATE TABLE, CREATE MATERIALIZED VIEW, or ALTER TABLE
		var (
			isCreateTable            = strings.HasPrefix(upperLine, "CREATE TABLE")
			isCreateMaterializedView = strings.HasPrefix(upperLine, "CREATE MATERIALIZED VIEW")
			isAlterTable             = strings.HasPrefix(upperLine, "ALTER TABLE")
		)

		//nolint:nestif // Complex SQL statement parsing.
		if isCreateTable || isCreateMaterializedView || isAlterTable {
			// Extract object name and qualify it
			// Pattern: CREATE [MATERIALIZED VIEW|TABLE] [IF NOT EXISTS] name [on cluster|to|engine|(...)]
			// Pattern: ALTER TABLE name [on cluster] ...
			var (
				parts      = strings.Fields(trimmed)
				objNameIdx = 2 // Default for CREATE TABLE and ALTER TABLE: 0=CREATE/ALTER, 1=TABLE, 2=table_name
			)

			if isCreateMaterializedView {
				objNameIdx = 3 // CREATE MATERIALIZED VIEW: 0=CREATE, 1=MATERIALIZED, 2=VIEW, 3=view_name
			}

			// Check for "IF NOT EXISTS" clause (only for CREATE TABLE).
			if isCreateTable && len(parts) >= 5 && strings.EqualFold(parts[2], "IF") &&
				strings.EqualFold(parts[3], "NOT") && strings.EqualFold(parts[4], "EXISTS") {
				objNameIdx = 5 // Skip to table name after "IF NOT EXISTS"
			}

			if len(parts) > objNameIdx {
				objName := parts[objNameIdx]

				// Only qualify if object name doesn't already have database prefix.
				if !strings.Contains(objName, ".") && !strings.HasPrefix(objName, "`"+dbName) {
					// Replace the unqualified object name with qualified version.
					qualifiedName := fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(objName, "`"))

					// Build the new line with qualified object name.
					newLine := strings.Replace(line, objName, qualifiedName, 1)

					// Also check if there's an AS clause on the same line that needs qualification
					// Pattern: CREATE TABLE name ... AS other_table.
					if strings.Contains(strings.ToUpper(newLine), " AS ") {
						asIndex := strings.Index(strings.ToUpper(newLine), " AS ")
						if asIndex != -1 {
							beforeAS := newLine[:asIndex+4] // Include " AS " or " as "
							afterAS := strings.TrimSpace(newLine[asIndex+4:])

							// Extract table name (first word after AS)
							asParts := strings.Fields(afterAS)
							if len(asParts) > 0 {
								asTableName := asParts[0]
								// Only qualify if not already qualified
								if !strings.Contains(asTableName, ".") && !strings.HasPrefix(asTableName, "`"+dbName) {
									asQualifiedName := fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(asTableName, "`"))
									// Replace AS table name
									afterAS = strings.Replace(afterAS, asTableName, asQualifiedName, 1)
									newLine = beforeAS + afterAS
								}
							}
						}
					}

					result = append(result, newLine)

					continue
				}
			}
		}

		// Qualify AS clause on its own line: AS table_name → AS dbName.table_name.
		// This handles multi-line CREATE TABLE ... AS statements.
		// Skip if AS is followed by SELECT (it's a query, not a table reference).
		if strings.HasPrefix(upperLine, "AS ") && !strings.Contains(line, ",") &&
			!strings.Contains(upperLine, "FIXEDSTRING") && !strings.Contains(upperLine, "CODEC") &&
			!strings.HasPrefix(upperLine, "AS SELECT") {
			parts := strings.Fields(trimmed)
			if len(parts) >= 2 {
				tableName := parts[1]
				// Only qualify if not already qualified
				if !strings.Contains(tableName, ".") && !strings.HasPrefix(tableName, "`"+dbName) {
					var (
						qualifiedName = fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(tableName, "`"))
						newLine       = strings.Replace(line, tableName, qualifiedName, 1)
					)

					result = append(result, newLine)

					continue
				}
			}
		}

		// Qualify TO clause in materialized views: TO table_name → TO dbName.table_name
		// Only if it looks like a TO clause (not a column definition ending with comma/type)
		if strings.HasPrefix(upperLine, "TO ") && !strings.Contains(line, ",") &&
			!strings.Contains(upperLine, "FIXEDSTRING") && !strings.Contains(upperLine, "CODEC") {
			parts := strings.Fields(trimmed)
			if len(parts) >= 2 {
				tableName := parts[1]
				// Only qualify if not already qualified
				if !strings.Contains(tableName, ".") && !strings.HasPrefix(tableName, "`"+dbName) {
					qualifiedName := fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(tableName, "`"))
					newLine := strings.Replace(line, tableName, qualifiedName, 1)
					result = append(result, newLine)
					continue
				}
			}
		}

		// Qualify FROM clause: FROM table_name → FROM dbName.table_name
		// Only if it looks like a FROM clause in a SELECT/query context (not a column name)
		// Skip if line contains comma (likely column definition) or data types
		if strings.HasPrefix(upperLine, "FROM ") && !strings.Contains(line, ",") &&
			!strings.Contains(upperLine, "FIXEDSTRING") && !strings.Contains(upperLine, "CODEC") {
			parts := strings.Fields(trimmed)
			if len(parts) >= 2 {
				tableName := parts[1]
				// Only qualify if not already qualified and not a function/subquery
				if !strings.Contains(tableName, ".") && !strings.HasPrefix(tableName, "`"+dbName) &&
					!strings.HasPrefix(tableName, "(") {
					qualifiedName := fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(tableName, "`"))
					newLine := strings.Replace(line, tableName, qualifiedName, 1)
					result = append(result, newLine)
					continue
				}
			}
		}

		// Qualify AS clause in CREATE TABLE statements: AS table_name → AS dbName.table_name
		// Pattern: CREATE TABLE ... AS table_name (not SELECT ... AS alias)
		// Only match if line contains CREATE TABLE and AS, or ends with table name after AS
		if strings.Contains(upperLine, " AS ") && //nolint:nestif // CREATE TABLE AS parsing - refactoring risky
			(strings.Contains(upperLine, "CREATE TABLE") || strings.Contains(upperLine, "ENGINE")) {
			// Find " AS " and qualify the table name after it
			asIndex := strings.Index(upperLine, " AS ")
			if asIndex != -1 {
				var (
					beforeAS = line[:strings.Index(line, " AS ")+4] // Include " AS "
					afterAS  = strings.TrimSpace(line[strings.Index(line, " AS ")+4:])
					parts    = strings.Fields(afterAS)
				)

				if len(parts) > 0 {
					tableName := parts[0]

					// Only qualify if not already qualified
					if !strings.Contains(tableName, ".") && !strings.HasPrefix(tableName, "`"+dbName) {
						qualifiedName := fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(tableName, "`"))
						// Replace tableName with qualifiedName in afterAS
						afterAS = strings.Replace(afterAS, tableName, qualifiedName, 1)
						newLine := beforeAS + afterAS
						result = append(result, newLine)

						continue
					}
				}
			}
		}

		// Keep line as-is if not matched above
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}
