package database

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethpandaops/xatu-cbt/pkg/config"
	"github.com/sirupsen/logrus"
)

// Manager handles dual-cluster database lifecycle
// - xatu cluster: Network databases (mainnet, sepolia) with external data
// - cbt cluster: Ephemeral test databases with transformations
type Manager interface {
	Start(ctx context.Context) error
	Stop() error
	PrepareNetworkDatabase(ctx context.Context, network string) error
	CreateTestDatabase(ctx context.Context, network, spec string) (string, error)
	DropDatabase(ctx context.Context, dbName string) error
	LoadParquetData(ctx context.Context, network string, dataFiles map[string]string) error
}

type manager struct {
	xatuConnStr        string // Connection to xatu-clickhouse cluster (external data)
	cbtConnStr         string // Connection to xatu-cbt-clickhouse cluster (transformations)
	migrationRunner    MigrationRunner
	xatuMigrationDir   string // Path to xatu migrations directory
	forceRebuild       bool   // Force rebuild of xatu cluster even if already prepared
	log                logrus.FieldLogger

	xatuConn *sql.DB // Connection to xatu cluster
	cbtConn  *sql.DB // Connection to cbt cluster
}

const (
	maxParquetLoadWorkers = 10
	queryTimeout          = 5 * time.Minute
)

// NewManager creates a new dual-cluster database manager
func NewManager(xatuConnStr, cbtConnStr string, migrationRunner MigrationRunner, xatuMigrationDir string, forceRebuild bool, log logrus.FieldLogger) Manager {
	return &manager{
		xatuConnStr:      xatuConnStr,
		cbtConnStr:       cbtConnStr,
		migrationRunner:  migrationRunner,
		xatuMigrationDir: xatuMigrationDir,
		forceRebuild:     forceRebuild,
		log:              log.WithField("component", "database_manager"),
	}
}

// Start opens connections to both ClickHouse clusters
func (m *manager) Start(ctx context.Context) error {
	m.log.Debug("starting database manager")

	// Connect to xatu cluster (external data)
	xatuConn, err := sql.Open("clickhouse", m.xatuConnStr)
	if err != nil {
		return fmt.Errorf("opening xatu clickhouse connection: %w", err)
	}

	if err := xatuConn.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging xatu clickhouse: %w", err)
	}

	// Connect to cbt cluster (transformations)
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
	m.log.Info("database manager started (dual-cluster mode)")

	return nil
}

// Stop closes both connections
func (m *manager) Stop() error {
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
		return fmt.Errorf("errors closing connections: %v", errs)
	}

	return nil
}

// PrepareNetworkDatabase ensures required databases exist in xatu cluster and runs migrations
// This is called once per test suite (not per test) to prepare the external data cluster
// Network parameter is used for tracking only - all data goes into 'default' database in xatu cluster
func (m *manager) PrepareNetworkDatabase(ctx context.Context, network string) error {
	m.log.WithField("network", network).Info("preparing xatu cluster databases")
	start := time.Now()

	// Xatu migrations use multiple databases: default, tmp, admin, dbt
	databases := []string{"default", "tmp", "admin", "dbt"}

	// Create/ensure all required databases exist in xatu cluster
	// Note: Use xatu_cluster (1 shard, 2 replicas) not cluster_2S_1R (which is for cbt cluster)
	for _, db := range databases {
		createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER %s", db, config.XatuClusterName)

		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		if _, err := m.xatuConn.ExecContext(queryCtx, createSQL); err != nil {
			cancel()
			return fmt.Errorf("creating %s database: %w", db, err)
		}
		cancel()
	}

	// Check if xatu cluster is already prepared (skip expensive migration if so)
	alreadyPrepared := !m.forceRebuild && m.isXatuClusterPrepared(ctx)

	if alreadyPrepared {
		m.log.Info("xatu cluster already prepared, skipping clear and migrations (use --force-rebuild to override)")
	} else {
		// Clear all tables in default, admin, and dbt databases (for fresh test suite run)
		// When force rebuilding, also clear schema_migrations tables to force re-run of migrations
		for _, db := range []string{"default", "admin", "dbt"} {
			m.log.WithField("database", db).Debug("clearing database tables")
			if err := m.clearNetworkTables(ctx, db, m.forceRebuild); err != nil {
				m.log.WithError(err).WithField("database", db).Warn("failed to clear tables (non-fatal)")
			}
		}

		// Run xatu migrations in xatu cluster default database
		if m.xatuMigrationDir != "" {
			xatuMigrationStart := time.Now()
			m.log.Info("running xatu migrations in xatu cluster")

			// No prefix needed - these are the primary tables in xatu cluster
			xatuMigrationRunner := NewMigrationRunner(m.xatuMigrationDir, "", m.log)
			if err := xatuMigrationRunner.RunMigrations(ctx, m.xatuConn, "default"); err != nil {
				return fmt.Errorf("running xatu migrations: %w", err)
			}

			m.log.WithFields(logrus.Fields{
				"duration": time.Since(xatuMigrationStart),
			}).Info("xatu migrations applied to xatu cluster")
		}
	}

	m.log.WithFields(logrus.Fields{
		"network":        network,
		"total_duration": time.Since(start),
		"skipped":        alreadyPrepared,
	}).Info("xatu cluster databases ready")

	return nil
}

// isXatuClusterPrepared checks if xatu cluster has already been set up with migrations
func (m *manager) isXatuClusterPrepared(ctx context.Context) bool {
	// Check if schema_migrations_default table exists and has migrations
	// Note: golang-migrate creates table with format schema_migrations_{database_name}
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var count int
	checkSQL := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM system.tables
		WHERE database = 'default'
		AND name = '%s%s'
	`, config.SchemaMigrationsPrefix, config.DefaultDatabase)
	err := m.xatuConn.QueryRowContext(queryCtx, checkSQL).Scan(&count)

	if err != nil || count == 0 {
		return false
	}

	// Check if migrations table has entries
	queryCtx2, cancel2 := context.WithTimeout(ctx, queryTimeout)
	defer cancel2()

	var migrationCount int
	countSQL := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s.%s%s
	`, config.DefaultDatabase, config.SchemaMigrationsPrefix, config.DefaultDatabase)
	err = m.xatuConn.QueryRowContext(queryCtx2, countSQL).Scan(&migrationCount)

	return err == nil && migrationCount > 0
}

// clearNetworkTables drops all tables and views in a network database to ensure clean slate for migrations
// If clearMigrations is true, also drops schema_migrations tables (used with --force-rebuild)
func (m *manager) clearNetworkTables(ctx context.Context, network string, clearMigrations bool) error {
	// Get list of all tables, materialized views, and views
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	rows, err := m.xatuConn.QueryContext(queryCtx,
		fmt.Sprintf("SELECT name, engine FROM system.tables WHERE database = '%s'", network))
	if err != nil {
		return fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	var objects []struct {
		name   string
		engine string
	}
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

	// Drop each object (tables, materialized views, views)
	// Note: Use xatu_cluster for xatu-clickhouse ON CLUSTER operations
	// Skip schema_migrations tables to preserve migration state (unless force rebuilding)
	for _, obj := range objects {
		// Skip schema_migrations tables unless force rebuilding
		// golang-migrate creates tables with format schema_migrations_{database_name}
		isSchemaMigrations := obj.name == config.SchemaMigrationsPrefix[:len(config.SchemaMigrationsPrefix)-1] || obj.name == config.SchemaMigrationsPrefix+config.DefaultDatabase
		if isSchemaMigrations && !clearMigrations {
			continue
		}

		var dropSQL string
		if obj.engine == "MaterializedView" {
			dropSQL = fmt.Sprintf("DROP VIEW IF EXISTS `%s`.`%s` ON CLUSTER %s SYNC", network, obj.name, config.XatuClusterName)
		} else {
			dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER %s SYNC", network, obj.name, config.XatuClusterName)
		}

		queryCtx2, cancel2 := context.WithTimeout(ctx, queryTimeout)
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

// CreateTestDatabase creates an ephemeral test database in CBT cluster for transformations
func (m *manager) CreateTestDatabase(ctx context.Context, network, spec string) (string, error) {
	dbName := m.generateDatabaseName(network, spec)

	m.log.WithField("database", dbName).Info("creating test database in cbt cluster")
	start := time.Now()

	// Create database in CBT cluster (transformations only)
	createSQL := fmt.Sprintf("CREATE DATABASE `%s` ON CLUSTER %s", dbName, config.CBTClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	if _, err := m.cbtConn.ExecContext(queryCtx, createSQL); err != nil {
		return "", fmt.Errorf("creating database: %w", err)
	}

	// Run xatu-cbt migrations in CBT cluster
	migrationStart := time.Now()
	if err := m.migrationRunner.RunMigrations(ctx, m.cbtConn, dbName); err != nil {
		_ = m.DropDatabase(ctx, dbName)
		return "", fmt.Errorf("running xatu-cbt migrations: %w", err)
	}

	m.log.WithFields(logrus.Fields{
		"database": dbName,
		"duration": time.Since(migrationStart),
	}).Info("xatu-cbt migrations applied to cbt cluster")

	m.log.WithFields(logrus.Fields{
		"database":       dbName,
		"total_duration": time.Since(start),
	}).Info("test database ready in cbt cluster")

	return dbName, nil
}

// DropDatabase drops a test database from CBT cluster and cleans up migrations table
func (m *manager) DropDatabase(ctx context.Context, dbName string) error {
	m.log.WithField("database", dbName).Debug("dropping test database from cbt cluster")

	// Drop the database from CBT cluster
	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s` ON CLUSTER %s", dbName, config.CBTClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	if _, err := m.cbtConn.ExecContext(queryCtx, dropSQL); err != nil {
		return fmt.Errorf("dropping database: %w", err)
	}

	// Clean up migrations table (golang-migrate creates these in default database)
	migrationTable := fmt.Sprintf("%s%s", config.SchemaMigrationsPrefix, dbName)
	dropMigrationsSQL := fmt.Sprintf("DROP TABLE IF EXISTS `default`.`%s`", migrationTable)

	queryCtx2, cancel2 := context.WithTimeout(ctx, queryTimeout)
	defer cancel2()

	if _, err := m.cbtConn.ExecContext(queryCtx2, dropMigrationsSQL); err != nil {
		m.log.WithError(err).WithField("table", migrationTable).Warn("failed to drop migrations table (non-fatal)")
	}

	m.log.WithField("database", dbName).Info("test database dropped from cbt cluster")

	return nil
}

// LoadParquetData loads parquet files into default database in xatu cluster
// network parameter is used for logging only - all data goes into 'default' database
func (m *manager) LoadParquetData(ctx context.Context, network string, dataFiles map[string]string) error {
	m.log.WithFields(logrus.Fields{
		"network": network,
		"files":   len(dataFiles),
	}).Info("loading parquet data into xatu cluster default database")

	start := time.Now()

	// Truncate external model tables before loading to avoid duplicates
	// This is needed because when smart skip is enabled, we don't clear tables
	// but we still want fresh data on each test run
	m.log.Info("truncating external model tables to ensure fresh data")
	for tableName := range dataFiles {
		if err := m.truncateExternalTable(ctx, tableName); err != nil {
			m.log.WithError(err).WithField("table", tableName).Warn("failed to truncate table (non-fatal)")
		}
	}

	// Create worker pool
	type job struct {
		tableName string
		filePath  string
	}

	jobs := make(chan job, len(dataFiles))
	errors := make(chan error, len(dataFiles))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < maxParquetLoadWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := m.loadParquetFile(ctx, j.tableName, j.filePath); err != nil {
					errors <- fmt.Errorf("loading %s: %w", j.tableName, err)
					return
				}
			}
		}()
	}

	// Enqueue jobs
	for tableName, filePath := range dataFiles {
		jobs <- job{tableName: tableName, filePath: filePath}
	}
	close(jobs)

	// Wait for completion
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		return err
	}

	m.log.WithFields(logrus.Fields{
		"files":    len(dataFiles),
		"duration": time.Since(start),
	}).Info("parquet data loaded into xatu cluster default database")

	return nil
}

// loadParquetFile loads a single parquet file into a table in xatu cluster default database
func (m *manager) loadParquetFile(ctx context.Context, tableName, filePath string) error {
	m.log.WithFields(logrus.Fields{
		"table": tableName,
		"file":  filePath,
	}).Debug("loading parquet file into xatu cluster default database")

	start := time.Now()

	// Load into _local table directly - ReplicatedMergeTree will automatically sync to all replicas
	localTableName := tableName + "_local"
	insertSQL := fmt.Sprintf(
		"INSERT INTO `default`.`%s` SELECT * FROM file('%s', Parquet)",
		localTableName, filePath,
	)

	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	if _, err := m.xatuConn.ExecContext(queryCtx, insertSQL); err != nil {
		return fmt.Errorf("inserting from parquet: %w", err)
	}

	m.log.WithFields(logrus.Fields{
		"table":    localTableName,
		"duration": time.Since(start),
	}).Debug("parquet file loaded into xatu cluster default database")

	return nil
}

// truncateExternalTable truncates an external model table to ensure fresh data on each load
func (m *manager) truncateExternalTable(ctx context.Context, tableName string) error {
	// Truncate the _local table - ReplicatedMergeTree will sync truncation to all replicas
	localTableName := tableName + "_local"
	truncateSQL := fmt.Sprintf("TRUNCATE TABLE `default`.`%s` ON CLUSTER %s SYNC", localTableName, config.XatuClusterName)

	m.log.WithField("table", localTableName).Debug("truncating external model table")

	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	if _, err := m.xatuConn.ExecContext(queryCtx, truncateSQL); err != nil {
		return fmt.Errorf("truncating table: %w", err)
	}

	return nil
}

// generateDatabaseName creates a unique database name for a test
func (m *manager) generateDatabaseName(network, spec string) string {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	// Add random suffix to ensure uniqueness in parallel execution
	randomBytes := make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to nanoseconds if random fails
		nanos := strconv.FormatInt(time.Now().UnixNano(), 10)
		return fmt.Sprintf("test_%s_%s_%s_%s", network, spec, timestamp, nanos[len(nanos)-6:])
	}

	randomSuffix := hex.EncodeToString(randomBytes)
	return fmt.Sprintf("test_%s_%s_%s_%s", network, spec, timestamp, randomSuffix)
}
