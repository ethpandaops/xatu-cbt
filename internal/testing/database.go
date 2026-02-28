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
	"github.com/ethpandaops/xatu-cbt/internal/infra"
	"github.com/ethpandaops/xatu-cbt/internal/testing/memfs"
	"github.com/fatih/color"
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
	validator        infra.Validator

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
		validator:        infra.NewValidator(cfg.SafeHostnames, log),
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

	// Validate hostnames before allowing any operations
	m.log.Debug("validating clickhouse hostnames")
	if err := m.validator.ValidateMultiple(ctx, xatuConn, cbtConn); err != nil {
		_ = xatuConn.Close()
		_ = cbtConn.Close()

		// Display big red warning table for hostname validation failures
		fmt.Println() // Blank line before warning
		displayHostnameValidationError(err, m.config.SafeHostnames)

		return fmt.Errorf("hostname validation failed - tests blocked for safety: %w", err)
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

	// Check if already prepared FIRST to avoid slow ON CLUSTER DDL
	alreadyPrepared := !m.forceRebuild && m.isXatuClusterPrepared(ctx)

	if !alreadyPrepared {
		// Only create databases if not already prepared
		databases := []string{"default", "tmp", "admin", "dbt"}

		for _, db := range databases {
			logCtx.WithField("database", db).Debug("creating database")

			createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER %s", db, config.XatuClusterName)

			queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
			if _, err := m.xatuConn.ExecContext(queryCtx, createSQL); err != nil {
				cancel()

				return fmt.Errorf("creating %s database: %w", db, err)
			}

			cancel()

			logCtx.WithField("database", db).Debug("database created")
		}
	}

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

// CreateCBTTemplate creates the CBT template database with migrations (run once at startup).
// Per-test databases will be cloned from this template to avoid re-running migrations.
func (m *DatabaseManager) CreateCBTTemplate(ctx context.Context, migrationDir string) error {
	templateDB := config.CBTTemplateDatabase

	logCtx := m.log.WithFields(logrus.Fields{
		"cluster":  "xatu-cbt",
		"database": templateDB,
	})

	// Check if template already exists and has migrations
	if !m.forceRebuild && m.isCBTTemplateReady(ctx) {
		logCtx.Info("CBT template already prepared, skipping migrations")
		return nil
	}

	logCtx.Info("preparing CBT template database")
	start := time.Now()

	// Create template database
	createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER %s", templateDB, config.CBTClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	if _, err := m.cbtConn.ExecContext(queryCtx, createSQL); err != nil {
		return fmt.Errorf("creating CBT template database: %w", err)
	}

	// Clear tables if force rebuild
	if m.forceRebuild {
		if err := m.clearCBTTemplateTables(ctx); err != nil {
			m.log.WithError(err).Warn("failed to clear CBT template tables (non-fatal)")
		}
	}

	logCtx.Info("running migrations")

	migrationStart := time.Now()
	if err := m.runMigrations(ctx, m.cbtConn, templateDB, migrationDir, ""); err != nil {
		return fmt.Errorf("running CBT template migrations: %w", err)
	}

	logCtx.WithFields(logrus.Fields{
		"duration": time.Since(migrationStart),
	}).Info("migrations applied")

	logCtx.WithFields(logrus.Fields{
		"total_duration": time.Since(start),
	}).Info("CBT template database ready")

	return nil
}

// isCBTTemplateReady checks if CBT template database exists with migrations applied.
func (m *DatabaseManager) isCBTTemplateReady(ctx context.Context) bool {
	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	var count int
	checkSQL := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		`SELECT COUNT(*)
		FROM system.tables
		WHERE database = 'default'
		AND name = '%s%s'`,
		config.SchemaMigrationsPrefix, config.CBTTemplateDatabase)
	err := m.cbtConn.QueryRowContext(queryCtx, checkSQL).Scan(&count)

	if err != nil || count == 0 {
		return false
	}

	queryCtx2, cancel2 := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel2()

	var migrationCount int
	countSQL := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		`SELECT COUNT(*)
		FROM default.%s%s`,
		config.SchemaMigrationsPrefix, config.CBTTemplateDatabase)
	err = m.cbtConn.QueryRowContext(queryCtx2, countSQL).Scan(&migrationCount)

	return err == nil && migrationCount > 0
}

// clearCBTTemplateTables drops all tables in CBT template database for force rebuild.
func (m *DatabaseManager) clearCBTTemplateTables(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	rows, err := m.cbtConn.QueryContext(queryCtx,
		fmt.Sprintf("SELECT name, engine FROM system.tables WHERE database = '%s'", config.CBTTemplateDatabase))
	if err != nil {
		return fmt.Errorf("querying template tables: %w", err)
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
		var dropSQL string
		if obj.engine == "MaterializedView" {
			dropSQL = fmt.Sprintf("DROP VIEW IF EXISTS `%s`.`%s` ON CLUSTER %s SYNC",
				config.CBTTemplateDatabase, obj.name, config.CBTClusterName)
		} else {
			dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER %s SYNC",
				config.CBTTemplateDatabase, obj.name, config.CBTClusterName)
		}

		queryCtx2, cancel2 := context.WithTimeout(ctx, m.config.QueryTimeout)
		if _, err := m.cbtConn.ExecContext(queryCtx2, dropSQL); err != nil {
			cancel2()
			m.log.WithError(err).WithField("object", obj.name).Warn("failed to drop template object (non-fatal)")
			continue
		}
		cancel2()
	}

	// Also drop migrations table
	dropMigrationsSQL := fmt.Sprintf("DROP TABLE IF EXISTS `default`.`%s%s`",
		config.SchemaMigrationsPrefix, config.CBTTemplateDatabase)

	queryCtx3, cancel3 := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel3()

	if _, err := m.cbtConn.ExecContext(queryCtx3, dropMigrationsSQL); err != nil {
		m.log.WithError(err).Warn("failed to drop template migrations table (non-fatal)")
	}

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

// cloneSpec describes a table clone operation with potentially different source and target names.
// This supports cross-database external models where the source table name differs from the model name
// (e.g., source observoor.cpu_utilization → target ext_123.observoor_cpu_utilization).
type cloneSpec struct {
	sourceDB    string
	sourceTable string
	targetTable string
}

// CloneExternalDatabase clones specific external tables from the xatu cluster to a per-test database.
// Supports cross-database external models where the source table lives in a non-default database
// (e.g., observoor.cpu_utilization) and needs to be cloned under the model name in the test database.
func (m *DatabaseManager) CloneExternalDatabase(ctx context.Context, testID string, tableRefs []ExternalTableRef) (string, error) {
	extDBName := config.ExternalDBPrefix + testID

	logCtx := m.log.WithFields(logrus.Fields{
		"cluster":  "xatu",
		"database": extDBName,
		"tables":   len(tableRefs),
	})

	start := time.Now()

	// Create the database
	createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER %s",
		extDBName, config.XatuClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	if _, err := m.xatuConn.ExecContext(queryCtx, createSQL); err != nil {
		cancel()
		return "", fmt.Errorf("creating external database: %w", err)
	}
	cancel()

	// Build clone specs for each external table ref.
	// Clone BOTH _local (ReplicatedMergeTree) AND distributed tables for each model.
	// For cross-database models, the source table name may differ from the target (model) name.
	specs := make([]cloneSpec, 0, len(tableRefs)*2)

	for _, ref := range tableRefs {
		sourceDB := config.DefaultDatabase
		sourceTable := ref.ModelName

		if ref.SourceDB != "" {
			sourceDB = ref.SourceDB
		}

		if ref.SourceTable != "" {
			sourceTable = ref.SourceTable
		}

		// Clone _local table first (ReplicatedMergeTree), then distributed table
		specs = append(specs,
			cloneSpec{sourceDB: sourceDB, sourceTable: sourceTable + "_local", targetTable: ref.ModelName + "_local"},
			cloneSpec{sourceDB: sourceDB, sourceTable: sourceTable, targetTable: ref.ModelName},
		)
	}

	// If no tables to clone, just return the empty database
	if len(specs) == 0 {
		logCtx.WithFields(logrus.Fields{
			"tables":   0,
			"duration": time.Since(start),
		}).Info("external database cloned (empty)")

		return extDBName, nil
	}

	// Clone tables in parallel with worker pool for speed
	const cloneWorkers = 20

	var (
		cloneWg   sync.WaitGroup
		cloneSem  = make(chan struct{}, cloneWorkers)
		cloneErrs = make(chan error, len(specs))
	)

	for _, spec := range specs {
		cloneWg.Add(1)

		go func(s cloneSpec) {
			defer cloneWg.Done()

			// Acquire semaphore
			cloneSem <- struct{}{}
			defer func() { <-cloneSem }()

			if cloneErr := m.cloneTableWithUniqueReplicaPath(ctx, m.xatuConn,
				s.sourceDB, extDBName, s.sourceTable, s.targetTable, config.XatuClusterName); cloneErr != nil {
				cloneErrs <- fmt.Errorf("cloning table %s.%s → %s: %w", s.sourceDB, s.sourceTable, s.targetTable, cloneErr)
			}
		}(spec)
	}

	cloneWg.Wait()
	close(cloneErrs)

	// Check for errors
	for cloneErr := range cloneErrs {
		_ = m.DropExternalDatabase(ctx, extDBName)

		return "", cloneErr
	}

	logCtx.WithFields(logrus.Fields{
		"tables":   len(specs),
		"duration": time.Since(start),
	}).Info("external database cloned")

	return extDBName, nil
}

// cloneTableWithUniqueReplicaPath clones a table, modifying ReplicatedMergeTree paths to be unique.
// sourceTable and targetTable may differ for cross-database external models where the source table name
// (e.g., cpu_utilization) differs from the model name used in the test database (e.g., observoor_cpu_utilization).
func (m *DatabaseManager) cloneTableWithUniqueReplicaPath(
	ctx context.Context,
	conn *sql.DB,
	sourceDB, targetDB, sourceTable, targetTable, clusterName string,
) error {
	// Get the CREATE TABLE statement from the source
	showSQL := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", sourceDB, sourceTable)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	var createStmt string
	if err := conn.QueryRowContext(queryCtx, showSQL).Scan(&createStmt); err != nil {
		return fmt.Errorf("getting CREATE TABLE statement for %s.%s: %w", sourceDB, sourceTable, err)
	}

	// Modify the statement:
	// 1. Replace database and table name in the CREATE TABLE line
	// 2. Replace ZooKeeper paths in ReplicatedMergeTree to include target database
	// 3. Rename table references when sourceTable != targetTable
	modifiedStmt := m.modifyCreateTableForClone(createStmt, sourceDB, targetDB, sourceTable, targetTable, clusterName)

	execCtx, execCancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer execCancel()

	if _, err := conn.ExecContext(execCtx, modifiedStmt); err != nil {
		return fmt.Errorf("executing modified CREATE TABLE for %s: %w", targetTable, err)
	}

	return nil
}

// modifyCreateTableForClone modifies a CREATE TABLE/VIEW statement for cloning to a new database.
// It updates the database/table name and modifies ReplicatedMergeTree ZooKeeper paths.
// When sourceTable != targetTable (cross-database models), it also renames table references.
func (m *DatabaseManager) modifyCreateTableForClone(
	createStmt, sourceDB, targetDB, sourceTable, targetTable, clusterName string,
) string {
	result := createStmt

	// Handle different CREATE statement types
	// Tables: CREATE TABLE default.table_name
	// Views: CREATE VIEW default.view_name
	// Materialized Views: CREATE MATERIALIZED VIEW default.mv_name

	// Replace TABLE references
	oldTableRef := fmt.Sprintf("CREATE TABLE %s.%s", sourceDB, sourceTable)
	newTableRef := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER %s",
		targetDB, targetTable, clusterName)
	result = strings.Replace(result, oldTableRef, newTableRef, 1)

	// Also handle backtick-quoted version for TABLE
	oldTableRefQuoted := fmt.Sprintf("CREATE TABLE `%s`.`%s`", sourceDB, sourceTable)
	result = strings.Replace(result, oldTableRefQuoted, newTableRef, 1)

	// Replace MATERIALIZED VIEW references
	oldMVRef := fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s", sourceDB, sourceTable)
	newMVRef := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS `%s`.`%s` ON CLUSTER %s",
		targetDB, targetTable, clusterName)
	result = strings.Replace(result, oldMVRef, newMVRef, 1)

	// Backtick-quoted MATERIALIZED VIEW
	oldMVRefQuoted := fmt.Sprintf("CREATE MATERIALIZED VIEW `%s`.`%s`", sourceDB, sourceTable)
	result = strings.Replace(result, oldMVRefQuoted, newMVRef, 1)

	// Replace VIEW references (non-materialized)
	oldViewRef := fmt.Sprintf("CREATE VIEW %s.%s", sourceDB, sourceTable)
	newViewRef := fmt.Sprintf("CREATE VIEW IF NOT EXISTS `%s`.`%s` ON CLUSTER %s",
		targetDB, targetTable, clusterName)
	result = strings.Replace(result, oldViewRef, newViewRef, 1)

	// Backtick-quoted VIEW
	oldViewRefQuoted := fmt.Sprintf("CREATE VIEW `%s`.`%s`", sourceDB, sourceTable)
	result = strings.Replace(result, oldViewRefQuoted, newViewRef, 1)

	// For Materialized Views with TO clause (e.g., CREATE MATERIALIZED VIEW ... TO default.target_table)
	// We need to update the TO target as well
	oldToRef := fmt.Sprintf(" TO %s.", sourceDB)
	newToRef := fmt.Sprintf(" TO `%s`.", targetDB)
	result = strings.ReplaceAll(result, oldToRef, newToRef)

	oldToRefQuoted := fmt.Sprintf(" TO `%s`.", sourceDB)
	result = strings.ReplaceAll(result, oldToRefQuoted, newToRef)

	// Modify ZooKeeper paths in ReplicatedMergeTree engine
	// There are multiple path formats used:
	// Format 1: /{cluster}/default/tables/{table_name}/{shard}
	// Format 2: /{cluster}/tables/{shard}/default/{table_name}
	// Format 3: /{cluster}/tmp/tables/{table_name}/{shard} (hardcoded 'tmp' paths)
	// We need to replace the database reference with the target database name

	// Format 1: /default/tables/
	oldPath1 := fmt.Sprintf("/%s/tables/", sourceDB)
	newPath1 := fmt.Sprintf("/%s/tables/", targetDB)
	result = strings.ReplaceAll(result, oldPath1, newPath1)

	// Format 2: /{shard}/default/ (in tables/{shard}/default/table_name paths)
	oldPath2 := fmt.Sprintf("/{shard}/%s/", sourceDB)
	newPath2 := fmt.Sprintf("/{shard}/%s/", targetDB)
	result = strings.ReplaceAll(result, oldPath2, newPath2)

	// Format 3: /tmp/tables/ (hardcoded paths that don't use database name)
	// Replace with target database to make it unique per-test
	result = strings.ReplaceAll(result, "/tmp/tables/", fmt.Sprintf("/%s/tables/", targetDB))

	// Also update Distributed engine references to point to the new database
	// Distributed engines reference: Distributed('{cluster}', 'default', 'table_local', ...)
	oldDistRef := fmt.Sprintf("'%s'", sourceDB)
	newDistRef := fmt.Sprintf("'%s'", targetDB)
	result = strings.ReplaceAll(result, oldDistRef, newDistRef)

	// For cross-database models where source table name differs from target table name,
	// rename the Distributed engine's local table reference.
	// Example: Distributed('{cluster}', 'ext_123', 'cpu_utilization_local', ...) needs to become
	//          Distributed('{cluster}', 'ext_123', 'observoor_cpu_utilization_local', ...)
	// Only target quoted references in the Distributed engine to avoid unintended global replacements.
	if sourceTable != targetTable {
		// Quoted reference (most common): 'cpu_utilization_local' → 'observoor_cpu_utilization_local'
		oldQuotedLocal := fmt.Sprintf("'%s_local'", sourceTable)
		newQuotedLocal := fmt.Sprintf("'%s_local'", targetTable)
		result = strings.ReplaceAll(result, oldQuotedLocal, newQuotedLocal)

		// Unquoted reference: cpu_utilization_local, → observoor_cpu_utilization_local,
		oldUnquotedLocal := fmt.Sprintf(", %s_local,", sourceTable)
		newUnquotedLocal := fmt.Sprintf(", %s_local,", targetTable)
		result = strings.ReplaceAll(result, oldUnquotedLocal, newUnquotedLocal)

		// Quoted base table ref: 'cpu_utilization' → 'observoor_cpu_utilization'
		oldQuotedBase := fmt.Sprintf("'%s'", sourceTable)
		newQuotedBase := fmt.Sprintf("'%s'", targetTable)
		result = strings.ReplaceAll(result, oldQuotedBase, newQuotedBase)
	}

	return result
}

// CloneCBTDatabase clones specific tables from the CBT template to a per-test database.
// If tableNames is empty, clones all tables (legacy behavior).
func (m *DatabaseManager) CloneCBTDatabase(ctx context.Context, testID string, tableNames []string) (string, error) {
	cbtDBName := config.CBTDBPrefix + testID

	logCtx := m.log.WithFields(logrus.Fields{
		"cluster":  "xatu-cbt",
		"database": cbtDBName,
		"tables":   len(tableNames),
	})

	start := time.Now()

	// Create the database
	createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER %s",
		cbtDBName, config.CBTClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	if _, err := m.cbtConn.ExecContext(queryCtx, createSQL); err != nil {
		cancel()
		return "", fmt.Errorf("creating cbt database: %w", err)
	}
	cancel()

	// Build list of tables to clone (transformation tables + admin tables)
	// For CBT, we need BOTH the local table AND the distributed table for each model.
	// CBT queries the distributed table which reads from the local table.
	tables := make([]tableInfo, 0, len(tableNames)*2+10) // *2 for local+distributed, +10 for admin

	for _, name := range tableNames {
		// Clone both the local table and the distributed table
		tables = append(tables,
			tableInfo{name: name + "_local"}, // e.g., fct_block_head_local
			tableInfo{name: name},            // e.g., fct_block_head (distributed)
		)
	}

	// Always include admin_* tables - CBT engine needs these for tracking bounds
	adminTables, err := m.listAdminTables(ctx, m.cbtConn, config.CBTTemplateDatabase)
	if err != nil {
		return "", fmt.Errorf("listing admin tables: %w", err)
	}

	tables = append(tables, adminTables...)

	// Clone tables in parallel with worker pool for speed
	const cloneWorkers = 20

	var (
		cloneWg   sync.WaitGroup
		cloneSem  = make(chan struct{}, cloneWorkers)
		cloneErrs = make(chan error, len(tables))
	)

	for _, table := range tables {
		cloneWg.Add(1)

		go func(t tableInfo) {
			defer cloneWg.Done()

			// Acquire semaphore
			cloneSem <- struct{}{}
			defer func() { <-cloneSem }()

			if cloneErr := m.cloneTableWithUniqueReplicaPath(ctx, m.cbtConn,
				config.CBTTemplateDatabase, cbtDBName, t.name, t.name, config.CBTClusterName); cloneErr != nil {
				cloneErrs <- fmt.Errorf("cloning table %s: %w", t.name, cloneErr)
			}
		}(table)
	}

	cloneWg.Wait()
	close(cloneErrs)

	// Check for errors
	for cloneErr := range cloneErrs {
		_ = m.DropCBTDatabase(ctx, cbtDBName)

		return "", cloneErr
	}

	logCtx.WithFields(logrus.Fields{
		"tables":   len(tables),
		"duration": time.Since(start),
	}).Info("cbt database cloned")

	return cbtDBName, nil
}

// DropExternalDatabase removes a per-test external database from xatu cluster.
func (m *DatabaseManager) DropExternalDatabase(ctx context.Context, dbName string) error {
	logCtx := m.log.WithFields(logrus.Fields{
		"cluster":  "xatu",
		"database": dbName,
	})

	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s` ON CLUSTER %s SYNC",
		dbName, config.XatuClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	if _, err := m.xatuConn.ExecContext(queryCtx, dropSQL); err != nil {
		return fmt.Errorf("dropping external database: %w", err)
	}

	logCtx.Info("external database dropped")

	return nil
}

// DropCBTDatabase removes a per-test CBT database from the CBT cluster.
func (m *DatabaseManager) DropCBTDatabase(ctx context.Context, dbName string) error {
	logCtx := m.log.WithFields(logrus.Fields{
		"cluster":  "xatu-cbt",
		"database": dbName,
	})

	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s` ON CLUSTER %s SYNC",
		dbName, config.CBTClusterName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	if _, err := m.cbtConn.ExecContext(queryCtx, dropSQL); err != nil {
		return fmt.Errorf("dropping CBT database: %w", err)
	}

	// Also drop migrations table if it exists
	migrationTable := fmt.Sprintf("%s%s", config.SchemaMigrationsPrefix, dbName)
	dropMigrationsSQL := fmt.Sprintf("DROP TABLE IF EXISTS `default`.`%s`", migrationTable)

	queryCtx2, cancel2 := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel2()

	if _, err := m.cbtConn.ExecContext(queryCtx2, dropMigrationsSQL); err != nil {
		logCtx.WithError(err).WithField("table", migrationTable).Warn("failed to drop migrations table (non-fatal)")
	}

	logCtx.Info("CBT database dropped")

	return nil
}

// tableInfo holds basic table metadata.
type tableInfo struct {
	name   string
	engine string
}

// listAdminTables returns all admin_* tables from a database.
// These tables are used by CBT engine for tracking bounds and state.
func (m *DatabaseManager) listAdminTables(ctx context.Context, conn *sql.DB, database string) ([]tableInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	//nolint:gosec // database name is controlled internally, not user input
	query := fmt.Sprintf(
		"SELECT name, engine FROM system.tables WHERE database = '%s' "+
			"AND name LIKE 'admin_%%'",
		database)

	rows, err := conn.QueryContext(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("querying admin tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tables := make([]tableInfo, 0, 10)

	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.name, &t.engine); err != nil {
			return nil, fmt.Errorf("scanning admin table info: %w", err)
		}

		tables = append(tables, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating admin tables: %w", err)
	}

	return tables, nil
}

// tableTimeRange holds the min/max DateTime range for an external table.
type tableTimeRange struct {
	table  string
	column string
	min    time.Time
	max    time.Time
}

// ValidateExternalData checks external tables after parquet loading.
// It validates that at least one table has data (some may be legitimately empty,
// e.g. pre-PeerDAS blob_sidecar tables in a post-PeerDAS time window) and that
// tables with data have overlapping time ranges. Non-overlapping ranges cause CBT
// to silently skip transformations because it cannot find a valid dependency intersection.
func (m *DatabaseManager) ValidateExternalData(ctx context.Context, database string, tables []string) error {
	ranges := make([]tableTimeRange, 0, len(tables))
	emptyTables := make([]string, 0)
	tablesWithData := 0

	for _, tableName := range tables {
		count, err := m.getExternalTableRowCount(ctx, database, tableName)
		if err != nil {
			return err
		}

		if count == 0 {
			emptyTables = append(emptyTables, tableName)
			m.log.WithFields(logrus.Fields{
				"database": database,
				"table":    tableName,
			}).Warn("external table has 0 rows after parquet load")

			continue
		}

		tablesWithData++

		m.log.WithFields(logrus.Fields{
			"database": database,
			"table":    tableName,
			"rows":     count,
		}).Debug("validated external table has data")

		// Get time range for overlap validation
		tr, err := m.getTableTimeRange(ctx, database, tableName)
		if err != nil {
			m.log.WithError(err).WithField("table", tableName).Debug("skipping time range check")

			continue
		}

		ranges = append(ranges, *tr)
	}

	if tablesWithData == 0 {
		return fmt.Errorf( //nolint:err113 // Dynamic validation error
			"all %d external tables have 0 rows after parquet load — test data is empty or broken: %s",
			len(tables), strings.Join(tables, ", "),
		)
	}

	if len(emptyTables) > 0 {
		m.log.WithFields(logrus.Fields{
			"empty_tables":     emptyTables,
			"tables_with_data": tablesWithData,
			"total_tables":     len(tables),
		}).Warn("some external tables are empty — this is expected for era-dependent models (e.g. pre/post PeerDAS)")
	}

	return m.validateTimeRangeOverlap(ranges)
}

// getExternalTableRowCount returns the row count for an external table.
func (m *DatabaseManager) getExternalTableRowCount(ctx context.Context, database, tableName string) (uint64, error) {
	query := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		"SELECT count() FROM `%s`.`%s`",
		database, tableName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	var count uint64

	err := m.xatuConn.QueryRowContext(queryCtx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("checking row count for %s.%s: %w", database, tableName, err)
	}

	return count, nil
}

// getTableTimeRange finds a DateTime column in the table and returns its min/max range.
func (m *DatabaseManager) getTableTimeRange(ctx context.Context, database, tableName string) (*tableTimeRange, error) {
	// Find a DateTime column via system.columns
	colQuery := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		"SELECT name FROM system.columns WHERE database = '%s' AND table = '%s'"+
			" AND type IN ('DateTime', 'DateTime64(3)')"+
			" AND name IN ('slot_start_date_time', 'event_date_time')"+
			" LIMIT 1",
		database, tableName)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	var col string

	err := m.xatuConn.QueryRowContext(queryCtx, colQuery).Scan(&col)
	if err != nil {
		return nil, fmt.Errorf("finding datetime column for %s.%s: %w", database, tableName, err)
	}

	// Query min/max of that column
	rangeQuery := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		"SELECT min(`%s`), max(`%s`) FROM `%s`.`%s`",
		col, col, database, tableName)

	rangeCtx, rangeCancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer rangeCancel()

	var minTime, maxTime time.Time

	err = m.xatuConn.QueryRowContext(rangeCtx, rangeQuery).Scan(&minTime, &maxTime)
	if err != nil {
		return nil, fmt.Errorf("querying time range for %s.%s.%s: %w", database, tableName, col, err)
	}

	return &tableTimeRange{
		table:  tableName,
		column: col,
		min:    minTime,
		max:    maxTime,
	}, nil
}

// validateTimeRangeOverlap checks that all external tables have overlapping time ranges.
// If tables don't overlap, CBT's dependency validator will never find a valid range
// and the transformation will silently never execute.
func (m *DatabaseManager) validateTimeRangeOverlap(ranges []tableTimeRange) error {
	if len(ranges) < 2 {
		return nil
	}

	// Find the intersection of all ranges: max of all mins, min of all maxes
	overlapStart := ranges[0].min
	overlapEnd := ranges[0].max

	for _, r := range ranges[1:] {
		if r.min.After(overlapStart) {
			overlapStart = r.min
		}

		if r.max.Before(overlapEnd) {
			overlapEnd = r.max
		}
	}

	if overlapStart.After(overlapEnd) {
		// Build a diagnostic message showing each table's range
		details := make([]string, 0, len(ranges))
		for _, r := range ranges {
			details = append(details, fmt.Sprintf(
				"  %s (%s): %s to %s",
				r.table, r.column,
				r.min.Format(time.RFC3339), r.max.Format(time.RFC3339),
			))
		}

		return fmt.Errorf( //nolint:err113 // Dynamic validation error
			"external tables have non-overlapping time ranges — CBT cannot find valid dependency intersection:\n%s",
			strings.Join(details, "\n"),
		)
	}

	// Log the overlap for debugging
	for _, r := range ranges {
		m.log.WithFields(logrus.Fields{
			"table":  r.table,
			"column": r.column,
			"min":    r.min.Format(time.RFC3339),
			"max":    r.max.Format(time.RFC3339),
		}).Debug("external table time range")
	}

	m.log.WithFields(logrus.Fields{
		"overlap_start": overlapStart.Format(time.RFC3339),
		"overlap_end":   overlapEnd.Format(time.RFC3339),
		"tables":        len(ranges),
	}).Debug("external tables have valid time overlap")

	return nil
}

// LoadParquetData loads parquet files into the specified database in xatu cluster.
func (m *DatabaseManager) LoadParquetData(ctx context.Context, database string, dataFiles map[string]string) error {
	logCtx := m.log.WithFields(logrus.Fields{
		"cluster":  "xatu",
		"database": database,
	})

	start := time.Now()

	type job struct {
		tableName string
		filePath  string
	}

	jobs := make(chan job, len(dataFiles))
	errChan := make(chan error, len(dataFiles))
	var wg sync.WaitGroup

	for i := 0; i < m.config.MaxParquetLoadWorkers; i++ {
		wg.Add(1)
		go func(db string) {
			defer wg.Done()
			for j := range jobs {
				if err := m.loadParquetFile(ctx, db, j.tableName, j.filePath); err != nil {
					errChan <- fmt.Errorf("loading %s: %w", j.tableName, err)
					return
				}
			}
		}(database)
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
func (m *DatabaseManager) loadParquetFile(ctx context.Context, database, tableName, filePath string) error {
	localTableName := tableName + "_local"
	// Use streaming settings to avoid loading entire parquet into memory:
	// - max_insert_block_size: flush every 10k rows (smaller batches)
	// - min_insert_block_size_bytes: flush when buffer hits 10MB
	// - input_format_parquet_max_block_size: read parquet in 8k row chunks
	// - max_memory_usage: limit query to 2GB
	insertSQL := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers and file path
		`INSERT INTO "%s"."%s"
		 SELECT * FROM file('%s', Parquet)
		 SETTINGS
		   max_insert_block_size = 10000,
		   min_insert_block_size_bytes = 10485760,
		   input_format_parquet_max_block_size = 8192,
		   max_memory_usage = 2000000000`,
		database, localTableName, filePath,
	)

	queryCtx, cancel := context.WithTimeout(ctx, m.config.QueryTimeout)
	defer cancel()

	if _, err := m.xatuConn.ExecContext(queryCtx, insertSQL); err != nil {
		return fmt.Errorf("inserting from parquet: %w", err)
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
	fmt.Println(red("  To allow, set environment variable in .env:"))
	newWhitelist := append(append([]string{}, whitelist...), hostname)
	exportCmd := fmt.Sprintf("XATU_CBT_SAFE_HOSTS=%q", strings.Join(newWhitelist, ","))
	fmt.Println(red("    " + exportCmd))
	fmt.Println()
}
