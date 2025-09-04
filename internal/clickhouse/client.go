// Package clickhouse provides ClickHouse database connection and management utilities
package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ethpandaops/xatu-cbt/internal/config"
)

// Connect establishes a connection to ClickHouse using native protocol
func Connect(cfg *config.Config) (driver.Conn, error) {
	// Use "default" database for initial connection
	// We'll create the network database after connecting
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.ClickhouseHost, cfg.ClickhouseNativePort)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: cfg.ClickhouseUsername,
			Password: cfg.ClickhousePassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     time.Second * 30,
		MaxOpenConns:    5,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Duration(10) * time.Minute,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return conn, nil
}

// CreateDatabase creates a database if it doesn't exist
func CreateDatabase(conn driver.Conn, dbName, cluster string) error {
	ctx := context.Background()

	// Build the query
	var query string
	if cluster != "" {
		query = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER '%s'", dbName, cluster)
	} else {
		query = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
	}

	// Execute the query
	if err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// CreateMigrationTables creates the schema migration tables
func CreateMigrationTables(conn driver.Conn, dbName, cluster string) error {
	ctx := context.Background()

	// Create local migration table
	localTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.schema_migrations_local %s
		(
			version Int64,
			dirty UInt8,
			sequence UInt64
		) Engine = %s
		ORDER BY sequence`,
		fmt.Sprintf("`%s`", dbName),
		getClusterClause(cluster),
		getEngineClause(cluster, dbName, "schema_migrations_local"),
	)

	if err := conn.Exec(ctx, localTableQuery); err != nil {
		return fmt.Errorf("failed to create schema_migrations_local table: %w", err)
	}

	// Create distributed migration table
	distributedTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.schema_migrations %s AS %s.schema_migrations_local
		ENGINE = Distributed('%s', '%s', schema_migrations_local, cityHash64(version))`,
		fmt.Sprintf("`%s`", dbName),
		getClusterClause(cluster),
		fmt.Sprintf("`%s`", dbName),
		getClusterName(cluster),
		dbName,
	)

	if err := conn.Exec(ctx, distributedTableQuery); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	return nil
}

// Helper functions for building cluster-aware queries
func getClusterClause(cluster string) string {
	if cluster != "" {
		return fmt.Sprintf("ON CLUSTER '%s'", cluster)
	}
	return ""
}

func getEngineClause(cluster, _, _ string) string {
	if cluster != "" {
		return `ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')`
	}
	// For single-node setup, use regular MergeTree
	return "MergeTree()"
}

func getClusterName(cluster string) string {
	if cluster != "" {
		return cluster
	}
	// Default cluster name for single-node
	return "default"
}

// TestConnection tests if we can connect to ClickHouse
func TestConnection(cfg *config.Config) error {
	conn, err := Connect(cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	// Connection successful if we get here
	return nil
}
