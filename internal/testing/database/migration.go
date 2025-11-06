package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/sirupsen/logrus"
)

// MigrationRunner executes database migrations using golang-migrate
type MigrationRunner interface {
	RunMigrations(ctx context.Context, conn *sql.DB, dbName string) error
}

type migrationRunner struct {
	migrationDir  string
	tablePrefix   string // Prefix for migrations table name (e.g., "xatu_")
	log           logrus.FieldLogger
}

// NewMigrationRunner creates a new migration runner with optional table name prefix
func NewMigrationRunner(log logrus.FieldLogger, migrationDir string, tablePrefix string) MigrationRunner {
	return &migrationRunner{
		migrationDir: migrationDir,
		tablePrefix:  tablePrefix,
		log:          log.WithField("component", "migration_runner"),
	}
}

// RunMigrations executes all pending migrations for a database
func (r *migrationRunner) RunMigrations(ctx context.Context, conn *sql.DB, dbName string) error {
	// ANTI-FLAKE #12: Respect context cancellation throughout
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	r.log.WithField("database", dbName).Debug("running migrations")

	// Create templated filesystem
	sourceFS, err := r.createTemplatedFS(dbName)
	if err != nil {
		return fmt.Errorf("creating templated migrations: %w", err)
	}

	// Create iofs source driver
	sourceDriver, err := iofs.New(sourceFS, ".")
	if err != nil {
		return fmt.Errorf("creating source driver: %w", err)
	}

	// Create ClickHouse database driver with context-aware connection
	// ANTI-FLAKE #11: Per-database migrations table prevents lock contention
	migrationsTableName := fmt.Sprintf("%sschema_migrations_%s", r.tablePrefix, dbName)
	dbDriver, err := clickhouse.WithInstance(conn, &clickhouse.Config{
		DatabaseName:          dbName,
		MigrationsTable:       migrationsTableName,                          // Per-database migrations table with prefix
		MultiStatementEnabled: true,                                         // Enable multi-statement support for migration files
		MultiStatementMaxSize: 1024 * 1024,                                  // 1MB max per statement
	})
	if err != nil {
		return fmt.Errorf("creating clickhouse driver: %w", err)
	}

	// Create migrate instance
	m, err := migrate.NewWithInstance("iofs", sourceDriver, dbName, dbDriver)
	if err != nil {
		return fmt.Errorf("creating migrate instance: %w", err)
	}

	// ANTI-FLAKE #11: Migration Lock Contention Prevention
	// golang-migrate uses database-level locking via migrations table
	// Since each test has ephemeral database, no lock contention possible
	// Lock table: schema_migrations_{dbName} is unique per test

	// Run migrations with context monitoring
	done := make(chan error, 1)
	go func() {
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			done <- fmt.Errorf("running migrations: %w", err)
			return
		}
		done <- nil
	}()

	// ANTI-FLAKE #12: Respect context cancellation during migration
	select {
	case <-ctx.Done():
		return fmt.Errorf("migration cancelled: %w", ctx.Err())
	case err := <-done:
		if err != nil {
			return err
		}
	}

	r.log.WithField("database", dbName).Debug("migrations completed successfully")

	return nil
}

// createTemplatedFS creates an in-memory filesystem with templated migration files
func (r *migrationRunner) createTemplatedFS(dbName string) (fs.FS, error) {
	r.log.WithField("database", dbName).Debug("creating templated migration filesystem")

	// Read all migration files
	entries, err := os.ReadDir(r.migrationDir)
	if err != nil {
		return nil, fmt.Errorf("reading migration directory: %w", err)
	}

	// Create in-memory filesystem
	files := make(map[string]string)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasSuffix(filename, ".sql") {
			continue
		}

		// Read migration file
		content, err := os.ReadFile(filepath.Join(r.migrationDir, filename))
		if err != nil {
			return nil, fmt.Errorf("reading migration file %s: %w", filename, err)
		}

		// Template the SQL
		templatedSQL := r.templateSQL(string(content), dbName)

		// If migration is empty after templating, add a no-op statement to avoid "Empty query" error
		if len(templatedSQL) == 0 {
			r.log.WithField("file", filename).Debug("empty migration file, adding no-op statement")
			templatedSQL = "SELECT 1; -- No-op migration"
		}

		files[filename] = templatedSQL
	}

	r.log.WithField("files", len(files)).Debug("templated migration files")

	// Create in-memory FS
	return &memFS{files: files}, nil
}

// templateSQL replaces ${NETWORK_NAME} placeholder and hardcoded 'default' database references with actual database name
func (r *migrationRunner) templateSQL(sql, dbName string) string {
	// Special case: if dbName is "default", don't template anything
	// This is for xatu migrations running in their native default database
	if dbName == "default" {
		return sql
	}

	// Replace placeholder
	templated := strings.ReplaceAll(sql, "${NETWORK_NAME}", dbName)

	// CRITICAL FIX #1: Replace hardcoded database names in Distributed engine definitions
	// Pattern: ENGINE = Distributed('{cluster}', default, table_name, rand())
	// Pattern: ENGINE = Distributed('{cluster}', dbt, table_name, cityHash64(...))
	// Pattern: ENGINE = Distributed('{cluster}', admin, table_name, ...)
	// Note: 'tmp' is kept separate as it's a staging database for safe migrations
	hardcodedDBs := []string{"default", "dbt", "admin"}
	for _, db := range hardcodedDBs {
		templated = strings.ReplaceAll(templated, fmt.Sprintf(", %s,", db), fmt.Sprintf(", %s,", dbName))
		templated = strings.ReplaceAll(templated, fmt.Sprintf(",%s,", db), fmt.Sprintf(",%s,", dbName))
	}

	// Replace 'tmp' with test-specific tmp database to avoid conflicts between parallel tests
	tmpDB := fmt.Sprintf("%s_tmp", dbName)
	templated = strings.ReplaceAll(templated, ", tmp,", fmt.Sprintf(", %s,", tmpDB))
	templated = strings.ReplaceAll(templated, ",tmp,", fmt.Sprintf(",%s,", tmpDB))

	// CRITICAL FIX #2: Qualify CREATE/ALTER/DROP statements with database name
	// Pattern: DROP TABLE [IF EXISTS] table_name ... → DROP TABLE [IF EXISTS] dbName.table_name ...
	// Pattern: CREATE TABLE table_name ON CLUSTER ... → CREATE TABLE dbName.table_name ON CLUSTER ...
	// Pattern: CREATE MATERIALIZED VIEW view_name ON CLUSTER ... → CREATE MATERIALIZED VIEW dbName.view_name ON CLUSTER ...
	// Pattern: ALTER TABLE table_name ... → ALTER TABLE dbName.table_name ...
	// This prevents ClickHouse from defaulting to 'default' database when using ON CLUSTER
	templated = r.qualifyCreateStatements(templated, dbName)

	// CRITICAL FIX #3: Replace database qualifiers in INSERT/SELECT statements
	// Replace hardcoded database references (default, dbt, admin) with test database
	for _, db := range hardcodedDBs {
		templated = strings.ReplaceAll(templated, fmt.Sprintf("%s.", db), fmt.Sprintf("%s.", dbName))
		templated = strings.ReplaceAll(templated, fmt.Sprintf("FROM %s ", db), fmt.Sprintf("FROM %s ", dbName))
		templated = strings.ReplaceAll(templated, fmt.Sprintf("INTO %s ", db), fmt.Sprintf("INTO %s ", dbName))
	}

	// Replace 'tmp' database with test-specific tmp database
	templated = strings.ReplaceAll(templated, "tmp.", fmt.Sprintf("%s.", tmpDB))
	templated = strings.ReplaceAll(templated, "FROM tmp ", fmt.Sprintf("FROM %s ", tmpDB))
	templated = strings.ReplaceAll(templated, "INTO tmp ", fmt.Sprintf("INTO %s ", tmpDB))

	// CRITICAL FIX #4: Replace database names in ReplicatedMergeTree ZooKeeper paths
	// Pattern: '/clickhouse/{installation}/{cluster}/default/tables/{table}/{shard}'
	// These paths must be unique per test to avoid replica conflicts
	// Handle 'default', 'dbt', and 'admin' separately from 'tmp'
	for _, db := range []string{"default", "dbt", "admin"} {
		oldPath := fmt.Sprintf("/{cluster}/%s/", db)
		newPath := fmt.Sprintf("/{cluster}/%s/", dbName)
		templated = strings.ReplaceAll(templated, oldPath, newPath)

		// Also match pattern for paths with database in different positions
		oldPath2 := fmt.Sprintf("/%s/tables/", db)
		newPath2 := fmt.Sprintf("/%s/tables/", dbName)
		templated = strings.ReplaceAll(templated, oldPath2, newPath2)
	}

	// Handle 'tmp' separately - it should use the _tmp database
	templated = strings.ReplaceAll(templated, "/{cluster}/tmp/", fmt.Sprintf("/{cluster}/%s/", tmpDB))
	templated = strings.ReplaceAll(templated, "/tmp/tables/", fmt.Sprintf("/%s/tables/", tmpDB))

	// Trim whitespace to avoid empty query errors
	return strings.TrimSpace(templated)
}

// qualifyCreateStatements adds database qualifier to DDL statements
// Also qualifies unqualified table references in TO and FROM clauses
// Handles patterns like:
//   DROP TABLE [IF EXISTS] table_name ... → DROP TABLE [IF EXISTS] dbName.table_name ...
//   TRUNCATE TABLE table_name ... → TRUNCATE TABLE dbName.table_name ...
//   CREATE TABLE table_name ON CLUSTER ... → CREATE TABLE dbName.table_name ON CLUSTER ...
//   CREATE MATERIALIZED VIEW view_name ... → CREATE MATERIALIZED VIEW dbName.view_name ...
//   ALTER TABLE table_name ON CLUSTER ... → ALTER TABLE dbName.table_name ON CLUSTER ...
//   TO table_name → TO dbName.table_name
//   FROM table_name → FROM dbName.table_name (if not already qualified)
func (r *migrationRunner) qualifyCreateStatements(sql, dbName string) string {
	lines := strings.Split(sql, "\n")
	result := make([]string, 0, len(lines))

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upperLine := strings.ToUpper(trimmed)

		// Check if this line starts with DROP TABLE or TRUNCATE TABLE
		// Pattern: DROP TABLE [IF EXISTS] table_name [on cluster '{cluster}'] [SYNC];
		// Pattern: TRUNCATE TABLE table_name [on cluster '{cluster}'];
		if strings.HasPrefix(upperLine, "DROP TABLE") || strings.HasPrefix(upperLine, "TRUNCATE TABLE") {
			parts := strings.Fields(trimmed)
			objNameIdx := 2 // Default: 0=DROP/TRUNCATE, 1=TABLE, 2=table_name

			// Check for "IF EXISTS" clause (only for DROP)
			if strings.HasPrefix(upperLine, "DROP TABLE") && len(parts) >= 5 &&
			   strings.ToUpper(parts[2]) == "IF" && strings.ToUpper(parts[3]) == "EXISTS" {
				objNameIdx = 4 // Skip to table name after "IF EXISTS"
			}

			if len(parts) > objNameIdx {
				objName := parts[objNameIdx]

				// Only qualify if table name doesn't already have database prefix
				if !strings.Contains(objName, ".") && !strings.HasPrefix(objName, "`"+dbName) {
					// Replace the unqualified table name with qualified version
					qualifiedName := fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(objName, "`"))
					newLine := strings.Replace(line, objName, qualifiedName, 1)
					result = append(result, newLine)
					continue
				}
			}
		}

		// Check if this line starts with CREATE TABLE, CREATE MATERIALIZED VIEW, or ALTER TABLE
		isCreateTable := strings.HasPrefix(upperLine, "CREATE TABLE")
		isCreateMaterializedView := strings.HasPrefix(upperLine, "CREATE MATERIALIZED VIEW")
		isAlterTable := strings.HasPrefix(upperLine, "ALTER TABLE")

		if isCreateTable || isCreateMaterializedView || isAlterTable {
			// Extract object name and qualify it
			// Pattern: CREATE [MATERIALIZED VIEW|TABLE] [IF NOT EXISTS] name [on cluster|to|engine|(...)]
			// Pattern: ALTER TABLE name [on cluster] ...

			parts := strings.Fields(trimmed)
			objNameIdx := 2 // Default for CREATE TABLE and ALTER TABLE: 0=CREATE/ALTER, 1=TABLE, 2=table_name

			if isCreateMaterializedView {
				objNameIdx = 3 // CREATE MATERIALIZED VIEW: 0=CREATE, 1=MATERIALIZED, 2=VIEW, 3=view_name
			}

			// Check for "IF NOT EXISTS" clause (only for CREATE TABLE)
			if isCreateTable && len(parts) >= 5 && strings.ToUpper(parts[2]) == "IF" &&
			   strings.ToUpper(parts[3]) == "NOT" && strings.ToUpper(parts[4]) == "EXISTS" {
				objNameIdx = 5 // Skip to table name after "IF NOT EXISTS"
			}

			if len(parts) > objNameIdx {
				objName := parts[objNameIdx]

				// Only qualify if object name doesn't already have database prefix
				if !strings.Contains(objName, ".") && !strings.HasPrefix(objName, "`"+dbName) {
					// Replace the unqualified object name with qualified version
					qualifiedName := fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(objName, "`"))

					// Build the new line with qualified object name
					newLine := strings.Replace(line, objName, qualifiedName, 1)

					// Also check if there's an AS clause on the same line that needs qualification
					// Pattern: CREATE TABLE name ... AS other_table
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

		// Qualify AS clause on its own line: AS table_name → AS dbName.table_name
		// This handles multi-line CREATE TABLE ... AS statements
		// Skip if AS is followed by SELECT (it's a query, not a table reference)
		if strings.HasPrefix(upperLine, "AS ") && !strings.Contains(line, ",") &&
		   !strings.Contains(upperLine, "FIXEDSTRING") && !strings.Contains(upperLine, "CODEC") &&
		   !strings.HasPrefix(upperLine, "AS SELECT") {
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
		if strings.Contains(upperLine, " AS ") &&
		   (strings.Contains(upperLine, "CREATE TABLE") || strings.Contains(upperLine, "ENGINE")) {
			// Find " AS " and qualify the table name after it
			asIndex := strings.Index(upperLine, " AS ")
			if asIndex != -1 {
				beforeAS := line[:strings.Index(line, " AS ")+4] // Include " AS "
				afterAS := strings.TrimSpace(line[strings.Index(line, " AS ")+4:])

				// Extract table name (first word after AS)
				parts := strings.Fields(afterAS)
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

// memFS is a simple in-memory filesystem implementation for migration files
type memFS struct {
	files map[string]string
}

func (m *memFS) Open(name string) (fs.File, error) {
	// Strip leading slash if present
	name = strings.TrimPrefix(name, "/")

	// Handle directory listing
	if name == "." || name == "" {
		return &memDir{fs: m}, nil
	}

	content, ok := m.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}

	return &memFile{
		name:    name,
		content: content,
		offset:  0,
	}, nil
}

// memFile represents a file in the in-memory filesystem
type memFile struct {
	name    string
	content string
	offset  int
}

func (f *memFile) Stat() (fs.FileInfo, error) {
	return &memFileInfo{
		name: f.name,
		size: int64(len(f.content)),
	}, nil
}

func (f *memFile) Read(p []byte) (int, error) {
	if f.offset >= len(f.content) {
		return 0, io.EOF
	}
	n := copy(p, f.content[f.offset:])
	f.offset += n
	if n > 0 && f.offset >= len(f.content) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Close() error {
	return nil
}

// memDir represents a directory in the in-memory filesystem
type memDir struct {
	fs      *memFS
	entries []fs.DirEntry
	offset  int
}

func (d *memDir) Stat() (fs.FileInfo, error) {
	return &memFileInfo{
		name:  ".",
		size:  0,
		isDir: true,
	}, nil
}

func (d *memDir) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("is a directory") //nolint:err113 // Standard error for directory read
}

func (d *memDir) Close() error {
	return nil
}

func (d *memDir) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.entries == nil {
		// Build entries list
		d.entries = make([]fs.DirEntry, 0, len(d.fs.files))
		for name := range d.fs.files {
			d.entries = append(d.entries, &memDirEntry{
				name: name,
				info: &memFileInfo{
					name: name,
					size: int64(len(d.fs.files[name])),
				},
			})
		}
	}

	if n <= 0 {
		// Return all entries
		entries := d.entries[d.offset:]
		d.offset = len(d.entries)
		return entries, nil
	}

	// Return n entries
	end := d.offset + n
	if end > len(d.entries) {
		end = len(d.entries)
	}

	entries := d.entries[d.offset:end]
	d.offset = end

	return entries, nil
}

// memFileInfo implements fs.FileInfo
type memFileInfo struct {
	name  string
	size  int64
	isDir bool
}

func (i *memFileInfo) Name() string      { return i.name }
func (i *memFileInfo) Size() int64       { return i.size }
func (i *memFileInfo) Mode() fs.FileMode {
	if i.isDir {
		return fs.ModeDir | 0755
	}
	return 0644
}
func (i *memFileInfo) ModTime() time.Time { return time.Time{} }
func (i *memFileInfo) IsDir() bool        { return i.isDir }
func (i *memFileInfo) Sys() interface{}   { return nil }

// memDirEntry implements fs.DirEntry
type memDirEntry struct {
	name string
	info fs.FileInfo
}

func (e *memDirEntry) Name() string               { return e.name }
func (e *memDirEntry) IsDir() bool                { return e.info.IsDir() }
func (e *memDirEntry) Type() fs.FileMode          { return e.info.Mode().Type() }
func (e *memDirEntry) Info() (fs.FileInfo, error) { return e.info, nil }
