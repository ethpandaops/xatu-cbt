package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/savid/xatu-cbt/pkg/clickhouse"
	"github.com/savid/xatu-cbt/pkg/config"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// DataLoader handles loading test data into ClickHouse
type DataLoader interface {
	LoadTestData(ctx context.Context, dataDir string) error
}

type dataLoader struct {
	log logrus.FieldLogger
}

// DataConfig defines the structure of a data configuration file
type DataConfig struct {
	NetworkColumn string   `yaml:"network_column"`
	URLs          []string `yaml:"urls"`
}

// NewDataLoader creates a new data loader instance
func NewDataLoader(log logrus.FieldLogger) DataLoader {
	return &dataLoader{
		log: log.WithField("component", "data_loader"),
	}
}

func (d *dataLoader) LoadTestData(ctx context.Context, dataDir string) error {
	d.log.WithField("dir", dataDir).Debug("Loading test data")

	files, err := filepath.Glob(filepath.Join(dataDir, "*.yaml"))
	if err != nil {
		return fmt.Errorf("failed to list data files: %w", err)
	}

	if len(files) == 0 {
		d.log.Debug("No data files found, skipping data ingestion")
		return nil
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create ClickHouse connection
	conn, err := clickhouse.Connect(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to clickhouse: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			d.log.WithError(err).Debug("Failed to close connection")
		}
	}()

	for _, file := range files {
		if err := d.loadDataFile(ctx, conn, file, cfg.Network); err != nil {
			return fmt.Errorf("failed to load data file %s: %w", file, err)
		}
	}

	return nil
}

func (d *dataLoader) loadDataFile(ctx context.Context, conn driver.Conn, file, network string) error {
	d.log.WithField("file", file).Debug("Processing data file")

	data, err := os.ReadFile(file) //nolint:gosec // file path is from controlled source
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var dataConfig DataConfig
	if err := yaml.Unmarshal(data, &dataConfig); err != nil {
		return fmt.Errorf("failed to parse yaml: %w", err)
	}

	// Extract table name from filename
	tableName := filepath.Base(file)
	tableName = tableName[:len(tableName)-len(filepath.Ext(tableName))]

	for _, url := range dataConfig.URLs {
		d.log.WithFields(logrus.Fields{
			"url":   url,
			"table": tableName,
		}).Debug("Ingesting parquet file")

		if err := d.ingestParquet(ctx, conn, url, tableName, dataConfig.NetworkColumn, network); err != nil {
			return fmt.Errorf("failed to ingest %s: %w", url, err)
		}
	}

	return nil
}

func (d *dataLoader) checkTableExists(ctx context.Context, conn driver.Conn, tableName string) {
	checkQuery := fmt.Sprintf("EXISTS TABLE default.%s", tableName)
	var exists uint8
	if err := conn.QueryRow(ctx, checkQuery).Scan(&exists); err != nil {
		d.log.WithFields(logrus.Fields{
			"table": tableName,
			"error": err,
		}).Warn("Failed to check if table exists")
		return
	}

	if exists == 0 {
		// Table doesn't exist, let's see what similar tables do exist
		similarQuery := fmt.Sprintf("SELECT name FROM system.tables WHERE database = 'default' AND name LIKE '%%%s%%'", tableName[:min(len(tableName)-3, 20)])
		rows, err := conn.Query(ctx, similarQuery)
		if err != nil {
			return
		}
		defer func() {
			if err := rows.Close(); err != nil {
				d.log.WithError(err).Debug("Failed to close rows")
			}
		}()

		var similarTables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				similarTables = append(similarTables, name)
			}
		}

		d.log.WithFields(logrus.Fields{
			"table":          tableName,
			"similar_tables": similarTables,
		}).Error("Table does not exist in default database")
	}
}

func (d *dataLoader) ingestParquet(ctx context.Context, conn driver.Conn, url, tableName, networkColumn, network string) error {
	// First check if the table exists (for better error reporting)
	d.checkTableExists(ctx, conn, tableName)

	// Build the INSERT query
	var query string
	if networkColumn != "" && network != "" {
		// Use REPLACE to override the network column value during insertion
		// This avoids issues with updating key columns
		query = fmt.Sprintf(`
			INSERT INTO default.%s 
			SELECT * REPLACE ('%s' AS %s)
			FROM url('%s', 'Parquet')
		`, tableName, network, networkColumn, url)
	} else {
		// Standard insert without modification
		query = fmt.Sprintf(`
			INSERT INTO default.%s 
			SELECT * FROM url('%s', 'Parquet')
		`, tableName, url)
	}

	// Execute the insert query
	if err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	d.log.WithField("table", tableName).Info("Successfully ingested parquet data")
	return nil
}
