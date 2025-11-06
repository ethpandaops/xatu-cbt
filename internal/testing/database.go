// Package testing provides end-to-end test orchestration and execution.
package testing

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver registration
	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/testing/memfs"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/sirupsen/logrus"
)

// DatabaseManager handles dual-cluster database lifecycle.
// - xatu cluster: Network databases (mainnet, sepolia) with external data
// - cbt cluster: Ephemeral test databases with transformations
type DatabaseManager struct {
	xatuConnStr      string
	cbtConnStr       string
	xatuMigrationDir string
	forceRebuild     bool
	log              logrus.FieldLogger
	config           *TestConfig

	xatuConn *sql.DB
	cbtConn  *sql.DB
}

// NewDatabaseManager creates a new dual-cluster database manager.
func NewDatabaseManager(
	log logrus.FieldLogger,
	cfg *TestConfig,
	xatuConnStr,
	cbtConnStr,
	xatuMigrationDir string,
	forceRebuild bool,
) *DatabaseManager {
	if cfg == nil {
		cfg = DefaultTestConfig()
	}

	return &DatabaseManager{
		xatuConnStr:      xatuConnStr,
		cbtConnStr:       cbtConnStr,
		xatuMigrationDir: xatuMigrationDir,
		forceRebuild:     forceRebuild,
		log:              log.WithField("component", "database_manager"),
		config:           cfg,
	}
}

// Start initializes database connections.
func (m *DatabaseManager) Start(ctx context.Context) error {
	m.log.Debug("starting database manager")

	xatuConn, err := sql.Open("clickhouse", m.xatuConnStr)
	if err != nil {
		return fmt.Errorf("opening xatu clickhouse connection: %w", err)
	}

	if pingErr := xatuConn.PingContext(ctx); pingErr != nil {
		return fmt.Errorf("pinging xatu clickhouse: %w", pingErr)
	}

	cbtConn, err := sql.Open("clickhouse", m.cbtConnStr)
	if err != nil {
		_ = xatuConn.Close()
		return fmt.Errorf("opening cbt clickhouse connection: %w", err)
	}

	if err := cbtConn.PingContext(ctx); err != nil {
		_ = xatuConn.Close()
		return fmt.Errorf("pinging cbt clickhouse: %w", err)
	}

	m.xatuConn = xatuConn
	m.cbtConn = cbtConn

	m.log.Info("database manager started")

	return nil
}

// Stop closes database connections.
func (m *DatabaseManager) Stop() error {
	m.log.Debug("stopping database manager")

	var errs []error

	if m.xatuConn != nil {
		if err := m.xatuConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing xatu connection: %w", err))
		}
	}

	if m.cbtConn != nil {
		if err := m.cbtConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing cbt connection: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs) //nolint:err113 // Include error list for debugging
	}

	return nil
}

// PrepareNetworkDatabase ensures required databases exist in xatu cluster and runs migrations.
func (m *DatabaseManager) PrepareNetworkDatabase(ctx context.Context, _ string) error {
	logCtx := m.log.WithField("cluster", "xatu")
	logCtx.Info("preparing database")

	start := time.Now()

	databases := []string{"default", "tmp", "admin", "dbt"}

	for _, db := range databases {
		createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER %s", db, config.XatuClusterName)

		queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
		if _, err := m.xatuConn.ExecContext(queryCtx, createSQL); err != nil {
			cancel()
			return fmt.Errorf("creating %s database: %w", db, err)
		}
		cancel()
	}

	alreadyPrepared := !m.forceRebuild && m.isXatuClusterPrepared(ctx)

	if alreadyPrepared { //nolint:nestif // Database preparation logic
		logCtx.Info("cluster already prepared")
	} else {
		for _, db := range []string{"default", "admin", "dbt"} {
			m.log.WithField("database", db).Debug("clearing database tables")
			if err := m.clearNetworkTables(ctx, db, m.forceRebuild); err != nil {
				m.log.WithError(err).WithField("database", db).Warn("failed to clear tables (non-fatal)")
			}
		}

		if m.xatuMigrationDir != "" {
			xatuMigrationStart := time.Now()
			logCtx.Info("running migrations")

			if err := m.runMigrations(ctx, m.xatuConn, "default", m.xatuMigrationDir, ""); err != nil {
				return fmt.Errorf("running xatu migrations: %w", err)
			}

			logCtx.WithFields(logrus.Fields{
				"duration": time.Since(xatuMigrationStart),
			}).Info("migrations applied")
		}
	}

	logCtx.WithFields(logrus.Fields{
		"total_duration": time.Since(start),
		"skipped":        alreadyPrepared,
	}).Info("xatu database ready")

	return nil
}

// isXatuClusterPrepared checks if xatu cluster has already been set up with migrations
func (m *DatabaseManager) isXatuClusterPrepared(ctx context.Context) bool {
	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	var count int
	checkSQL := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		`SELECT COUNT(*)
		FROM system.tables
		WHERE database = 'default'
		AND name = '%s%s'`,
		config.SchemaMigrationsPrefix, config.DefaultDatabase)
	err := m.xatuConn.QueryRowContext(queryCtx, checkSQL).Scan(&count)

	if err != nil || count == 0 {
		return false
	}

	queryCtx2, cancel2 := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel2()

	var migrationCount int
	countSQL := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		`SELECT COUNT(*)
		FROM %s.%s%s`,
		config.DefaultDatabase, config.SchemaMigrationsPrefix, config.DefaultDatabase)
	err = m.xatuConn.QueryRowContext(queryCtx2, countSQL).Scan(&migrationCount)

	return err == nil && migrationCount > 0
}

// clearNetworkTables drops all tables and views in a network database
func (m *DatabaseManager) clearNetworkTables(ctx context.Context, network string, clearMigrations bool) error {
	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	rows, err := m.xatuConn.QueryContext(queryCtx,
		fmt.Sprintf("SELECT name, engine FROM system.tables WHERE database = '%s'", network))
	if err != nil {
		return fmt.Errorf("querying tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	objects := make([]struct {
		name   string
		engine string
	}, 0, 10)
	for rows.Next() {
		var obj struct {
			name   string
			engine string
		}
		if err := rows.Scan(&obj.name, &obj.engine); err != nil {
			return fmt.Errorf("scanning table info: %w", err)
		}
		objects = append(objects, obj)
	}

	for _, obj := range objects {
		isSchemaMigrations := obj.name == config.SchemaMigrationsPrefix[:len(config.SchemaMigrationsPrefix)-1] ||
			obj.name == config.SchemaMigrationsPrefix+config.DefaultDatabase
		if isSchemaMigrations && !clearMigrations {
			continue
		}

		var dropSQL string
		if obj.engine == "MaterializedView" {
			dropSQL = fmt.Sprintf("DROP VIEW IF EXISTS `%s`.`%s` ON CLUSTER %s SYNC", network, obj.name, config.XatuClusterName)
		} else {
			dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER %s SYNC", network, obj.name, config.XatuClusterName)
		}

		queryCtx2, cancel2 := context.WithTimeout(ctx, m.config.QueryTimeout)
		if _, err := m.xatuConn.ExecContext(queryCtx2, dropSQL); err != nil {
			cancel2()
			m.log.WithError(err).WithFields(logrus.Fields{
				"object": obj.name,
				"engine": obj.engine,
			}).Warn("failed to drop object (non-fatal)")
			continue
		}
		cancel2()
	}

	return nil
}

// CreateTestDatabase creates an ephemeral test database in CBT cluster for transformations.
func (m *DatabaseManager) CreateTestDatabase(
	ctx context.Context,
	network,
	spec,
	migrationDir string,
) (string, error) {
	dbName := m.generateDatabaseName(network, spec)

	logCtx := m.log.WithFields(logrus.Fields{
		"cluster":  "xatu-cbt",
		"database": dbName,
	})
	logCtx.Info("preparing database")

	start := time.Now()

	createSQL := fmt.Sprintf("CREATE DATABASE `%s` ON CLUSTER %s", dbName, config.CBTClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	if _, err := m.cbtConn.ExecContext(queryCtx, createSQL); err != nil {
		return "", fmt.Errorf("creating database: %w", err)
	}

	logCtx.Info("running migrations")

	migrationStart := time.Now()
	if err := m.runMigrations(ctx, m.cbtConn, dbName, migrationDir, ""); err != nil {
		_ = m.DropDatabase(ctx, dbName)
		return "", fmt.Errorf("running xatu-cbt migrations: %w", err)
	}

	logCtx.WithFields(logrus.Fields{
		"duration": time.Since(migrationStart),
	}).Info("migrations applied")

	logCtx.WithFields(logrus.Fields{
		"total_duration": time.Since(start),
	}).Info("xatu-cbt database ready")

	return dbName, nil
}

// DropDatabase drops a test database from CBT cluster and cleans up migrations table.
func (m *DatabaseManager) DropDatabase(ctx context.Context, dbName string) error {
	logCtx := m.log.WithFields(logrus.Fields{
		"cluster":  "xatu-cbt",
		"database": dbName,
	})

	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s` ON CLUSTER %s", dbName, config.CBTClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	if _, err := m.cbtConn.ExecContext(queryCtx, dropSQL); err != nil {
		return fmt.Errorf("dropping database: %w", err)
	}

	migrationTable := fmt.Sprintf("%s%s", config.SchemaMigrationsPrefix, dbName)
	dropMigrationsSQL := fmt.Sprintf("DROP TABLE IF EXISTS `default`.`%s`", migrationTable)

	queryCtx2, cancel2 := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel2()

	if _, err := m.cbtConn.ExecContext(queryCtx2, dropMigrationsSQL); err != nil {
		logCtx.WithError(err).WithField("table", migrationTable).Warn("failed to drop migrations table (non-fatal)")
	}

	logCtx.Info("test database dropped")

	return nil
}

// LoadParquetData loads parquet files into default database in xatu cluster.
func (m *DatabaseManager) LoadParquetData(ctx context.Context, _ string, dataFiles map[string]string) error {
	logCtx := m.log.WithField("cluster", "xatu")

	start := time.Now()

	logCtx.Info("truncating tables")
	for tableName := range dataFiles {
		if err := m.truncateExternalTable(ctx, logCtx, tableName); err != nil {
			m.log.WithError(err).WithField("table", tableName).Warn("failed to truncate table (non-fatal)")
		}
	}

	type job struct {
		tableName string
		filePath  string
	}

	jobs := make(chan job, len(dataFiles))
	errChan := make(chan error, len(dataFiles))
	var wg sync.WaitGroup

	for i := 0; i < m.config.MaxParquetLoadWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := m.loadParquetFile(ctx, logCtx, j.tableName, j.filePath); err != nil {
					errChan <- fmt.Errorf("loading %s: %w", j.tableName, err)
					return
				}
			}
		}()
	}

	for tableName, filePath := range dataFiles {
		jobs <- job{tableName: tableName, filePath: filePath}
	}
	close(jobs)

	wg.Wait()
	close(errChan)

	for err := range errChan {
		return err
	}

	logCtx.WithFields(logrus.Fields{
		"files":    len(dataFiles),
		"duration": time.Since(start),
	}).Info("parquet data loaded")

	return nil
}

// loadParquetFile loads a single parquet file into a table
func (m *DatabaseManager) loadParquetFile(ctx context.Context, log *logrus.Entry, tableName, filePath string) error {
	log.WithFields(logrus.Fields{
		"table": tableName,
		"file":  filePath,
	}).Debug("loading parquet file")

	localTableName := tableName + "_local"
	insertSQL := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers and file path
		"INSERT INTO `default`.`%s` SELECT * FROM file('%s', Parquet)",
		localTableName, filePath,
	)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	if _, err := m.xatuConn.ExecContext(queryCtx, insertSQL); err != nil {
		return fmt.Errorf("inserting from parquet: %w", err)
	}

	return nil
}

// truncateExternalTable truncates an external model table
func (m *DatabaseManager) truncateExternalTable(ctx context.Context, log *logrus.Entry, tableName string) error {
	localTableName := tableName + "_local"
	truncateSQL := fmt.Sprintf("TRUNCATE TABLE `default`.`%s` ON CLUSTER %s SYNC", localTableName, config.XatuClusterName)

	log.WithField("table", localTableName).Debug("truncating table")

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	if _, err := m.xatuConn.ExecContext(queryCtx, truncateSQL); err != nil {
		return fmt.Errorf("truncating table: %w", err)
	}

	return nil
}

// generateDatabaseName creates a unique database name for a test
func (m *DatabaseManager) generateDatabaseName(network, spec string) string {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	randomBytes := make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		nanos := strconv.FormatInt(time.Now().UnixNano(), 10)
		return fmt.Sprintf("test_%s_%s_%s_%s", network, spec, timestamp, nanos[len(nanos)-6:])
	}

	randomSuffix := hex.EncodeToString(randomBytes)
	return fmt.Sprintf("test_%s_%s_%s_%s", network, spec, timestamp, randomSuffix)
}

// runMigrations executes all pending migrations for a database.
func (m *DatabaseManager) runMigrations(
	ctx context.Context,
	conn *sql.DB,
	dbName,
	migrationDir,
	tablePrefix string,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	sourceFS, err := m.createTemplatedFS(dbName, migrationDir)
	if err != nil {
		return fmt.Errorf("creating templated migrations: %w", err)
	}

	m.log.WithField("database", dbName).Debug("running migrations, please wait")

	sourceDriver, err := iofs.New(sourceFS, ".")
	if err != nil {
		return fmt.Errorf("creating source driver: %w", err)
	}

	migrationsTableName := fmt.Sprintf("%sschema_migrations_%s", tablePrefix, dbName)
	dbDriver, err := clickhouse.WithInstance(conn, &clickhouse.Config{
		DatabaseName:          dbName,
		MigrationsTable:       migrationsTableName,
		MultiStatementEnabled: true,
		MultiStatementMaxSize: 1024 * 1024,
	})
	if err != nil {
		return fmt.Errorf("creating clickhouse driver: %w", err)
	}

	mig, err := migrate.NewWithInstance("iofs", sourceDriver, dbName, dbDriver)
	if err != nil {
		return fmt.Errorf("creating migrate instance: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		if err := mig.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			done <- fmt.Errorf("running migrations: %w", err)
			return
		}
		done <- nil
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("migration canceled: %w", ctx.Err())
	case err := <-done:
		if err != nil {
			return err
		}
	}

	return nil
}

// createTemplatedFS creates an in-memory filesystem with templated migration files.
func (m *DatabaseManager) createTemplatedFS(dbName, migrationDir string) (fs.FS, error) {
	m.log.WithField("database", dbName).Debug("creating in-memory migration filesystem")

	entries, err := os.ReadDir(migrationDir)
	if err != nil {
		return nil, fmt.Errorf("reading migration directory: %w", err)
	}

	filesystem := memfs.NewFS()

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasSuffix(filename, ".sql") {
			continue
		}

		content, err := os.ReadFile(filepath.Join(migrationDir, filename)) //nolint:gosec // G304: Reading migration files from trusted directory
		if err != nil {
			return nil, fmt.Errorf("reading migration file %s: %w", filename, err)
		}

		templatedSQL := m.templateSQL(string(content), dbName)

		if templatedSQL == "" {
			m.log.WithField("file", filename).Debug("empty migration file, adding no-op statement")
			templatedSQL = "SELECT 1; -- No-op migration"
		}

		filesystem.WriteFile(filename, templatedSQL)
	}

	m.log.WithField("files", len(entries)).Debug("templated migration files")

	return filesystem, nil
}

// templateSQL replaces database placeholders in SQL migration content
//
//nolint:gocyclo // SQL parsing logic requires multiple branches
func (m *DatabaseManager) templateSQL(sqlTemplate, dbName string) string {
	if dbName == "default" {
		return sqlTemplate
	}

	templated := strings.ReplaceAll(sqlTemplate, "${NETWORK_NAME}", dbName)

	hardcodedDBs := []string{"default", "dbt", "admin"}
	for _, db := range hardcodedDBs {
		templated = strings.ReplaceAll(templated, fmt.Sprintf(", %s,", db), fmt.Sprintf(", %s,", dbName))
		templated = strings.ReplaceAll(templated, fmt.Sprintf(",%s,", db), fmt.Sprintf(",%s,", dbName))
	}

	tmpDB := fmt.Sprintf("%s_tmp", dbName)
	templated = strings.ReplaceAll(templated, ", tmp,", fmt.Sprintf(", %s,", tmpDB))
	templated = strings.ReplaceAll(templated, ",tmp,", fmt.Sprintf(",%s,", tmpDB))

	templated = m.qualifyCreateStatements(templated, dbName)

	for _, db := range hardcodedDBs {
		templated = strings.ReplaceAll(templated, fmt.Sprintf("%s.", db), fmt.Sprintf("%s.", dbName))
		templated = strings.ReplaceAll(templated, fmt.Sprintf("FROM %s ", db), fmt.Sprintf("FROM %s ", dbName))
		templated = strings.ReplaceAll(templated, fmt.Sprintf("INTO %s ", db), fmt.Sprintf("INTO %s ", dbName))
	}

	templated = strings.ReplaceAll(templated, "tmp.", fmt.Sprintf("%s.", tmpDB))
	templated = strings.ReplaceAll(templated, "FROM tmp ", fmt.Sprintf("FROM %s ", tmpDB))
	templated = strings.ReplaceAll(templated, "INTO tmp ", fmt.Sprintf("INTO %s ", tmpDB))

	for _, db := range []string{"default", "dbt", "admin"} {
		oldPath := fmt.Sprintf("/{cluster}/%s/", db)
		newPath := fmt.Sprintf("/{cluster}/%s/", dbName)
		templated = strings.ReplaceAll(templated, oldPath, newPath)

		oldPath2 := fmt.Sprintf("/%s/tables/", db)
		newPath2 := fmt.Sprintf("/%s/tables/", dbName)
		templated = strings.ReplaceAll(templated, oldPath2, newPath2)
	}

	templated = strings.ReplaceAll(templated, "/{cluster}/tmp/", fmt.Sprintf("/{cluster}/%s/", tmpDB))
	templated = strings.ReplaceAll(templated, "/tmp/tables/", fmt.Sprintf("/%s/tables/", tmpDB))

	return strings.TrimSpace(templated)
}

// qualifyCreateStatements adds database qualifier to DDL statements
func (m *DatabaseManager) qualifyCreateStatements(sqlContent, dbName string) string {
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

		if qualifiedLine, handled := qualifyDDLStatement(line, upperLine, parts, dbName); handled {
			result = append(result, qualifiedLine)
			continue
		}

		if qualifiedLine, handled := qualifyClauseLine(line, upperLine, parts, dbName); handled {
			result = append(result, qualifiedLine)
			continue
		}

		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

// qualifyDDLStatement processes DROP, TRUNCATE, CREATE, and ALTER statements.
//
//nolint:gocyclo // SQL parsing logic requires multiple branches
func qualifyDDLStatement(line, upperLine string, parts []string, dbName string) (string, bool) {
	var objNameIdx int

	switch {
	case strings.HasPrefix(upperLine, "DROP TABLE"):
		objNameIdx = 2
		if len(parts) >= 5 && strings.EqualFold(parts[2], "IF") && strings.EqualFold(parts[3], "EXISTS") {
			objNameIdx = 4
		}
	case strings.HasPrefix(upperLine, "TRUNCATE TABLE"):
		objNameIdx = 2
	case strings.HasPrefix(upperLine, "CREATE TABLE"):
		objNameIdx = 2
		if len(parts) >= 5 && strings.EqualFold(parts[2], "IF") &&
			strings.EqualFold(parts[3], "NOT") && strings.EqualFold(parts[4], "EXISTS") {
			objNameIdx = 5
		}
	case strings.HasPrefix(upperLine, "CREATE MATERIALIZED VIEW"):
		objNameIdx = 3
	case strings.HasPrefix(upperLine, "ALTER TABLE"):
		objNameIdx = 2
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

	if strings.HasPrefix(upperLine, "CREATE") && strings.Contains(strings.ToUpper(newLine), " AS ") {
		newLine = qualifyASClauseInline(newLine, dbName)
	}

	return newLine, true
}

// qualifyASClauseInline handles AS clauses within a line.
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

// qualifyClauseLine processes standalone AS, TO, and FROM clauses.
func qualifyClauseLine(line, upperLine string, parts []string, dbName string) (string, bool) {
	if strings.Contains(line, ",") || strings.Contains(upperLine, "FIXEDSTRING") || strings.Contains(upperLine, "CODEC") {
		return line, false
	}

	var tableName string

	switch {
	case strings.HasPrefix(upperLine, "AS ") && !strings.HasPrefix(upperLine, "AS SELECT"):
		if len(parts) < 2 {
			return line, false
		}
		tableName = parts[1]
	case strings.HasPrefix(upperLine, "TO "):
		if len(parts) < 2 {
			return line, false
		}
		tableName = parts[1]
	case strings.HasPrefix(upperLine, "FROM "):
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

// isQualified checks if a table name is already qualified.
func isQualified(name, dbName string) bool {
	return strings.Contains(name, ".") || strings.HasPrefix(name, "`"+dbName)
}

// qualifyName builds a fully qualified table name.
func qualifyName(name, dbName string) string {
	return fmt.Sprintf("`%s`.`%s`", dbName, strings.Trim(name, "`"))
}
