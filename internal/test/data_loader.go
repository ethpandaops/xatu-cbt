package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ethpandaops/xatu-cbt/internal/clickhouse"
	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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

// WorkItem represents a single URL to be ingested
type WorkItem struct {
	URL           string
	TableName     string
	NetworkColumn string
	Network       string
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

	// Phase 1: Collect all work items (URLs) from all files
	workItems, err := d.collectWorkItems(files, cfg.Network)
	if err != nil {
		return fmt.Errorf("failed to collect work items: %w", err)
	}

	if len(workItems) == 0 {
		d.log.Debug("No URLs found in data files, skipping data ingestion")
		return nil
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

	// Phase 2: Process work items using worker pool
	const numWorkers = 10
	d.log.WithFields(logrus.Fields{
		"urls":    len(workItems),
		"workers": numWorkers,
	}).Info("Starting worker pool for parallel data ingestion")

	// Create work channel
	workChan := make(chan WorkItem, len(workItems))

	// Feed all work items into the channel
	for _, item := range workItems {
		workChan <- item
	}
	close(workChan)

	// Use errgroup for worker management
	g, ctx := errgroup.WithContext(ctx)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		workerID := i + 1
		g.Go(func() error {
			return d.worker(ctx, workerID, conn, workChan)
		})
	}

	// Wait for all workers to complete
	if err := g.Wait(); err != nil {
		return err
	}

	d.log.Info("Completed parallel data ingestion")
	return nil
}

// collectWorkItems reads all data files and collects URLs to be processed
func (d *dataLoader) collectWorkItems(files []string, network string) ([]WorkItem, error) {
	var workItems []WorkItem

	for _, file := range files {
		d.log.WithField("file", file).Debug("Reading data file")

		data, err := os.ReadFile(file) //nolint:gosec // file path is from controlled source
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", file, err)
		}

		var dataConfig DataConfig
		if err := yaml.Unmarshal(data, &dataConfig); err != nil {
			return nil, fmt.Errorf("failed to parse yaml %s: %w", file, err)
		}

		// Extract table name from filename
		tableName := filepath.Base(file)
		tableName = tableName[:len(tableName)-len(filepath.Ext(tableName))]

		// Add each URL as a work item
		for _, url := range dataConfig.URLs {
			workItems = append(workItems, WorkItem{
				URL:           url,
				TableName:     tableName,
				NetworkColumn: dataConfig.NetworkColumn,
				Network:       network,
			})
		}
	}

	d.log.WithField("total_urls", len(workItems)).Info("Collected all work items")
	return workItems, nil
}

// worker processes work items from the channel
func (d *dataLoader) worker(ctx context.Context, id int, conn driver.Conn, workChan <-chan WorkItem) error {
	d.log.WithField("worker_id", id).Debug("Worker started")

	for {
		select {
		case <-ctx.Done():
			d.log.WithField("worker_id", id).Debug("Worker stopped due to context cancellation")
			return ctx.Err()
		case item, ok := <-workChan:
			if !ok {
				d.log.WithField("worker_id", id).Debug("Worker finished - no more work")
				return nil
			}

			d.log.WithFields(logrus.Fields{
				"worker_id": id,
				"url":       item.URL,
				"table":     item.TableName,
			}).Debug("Worker processing URL")

			if err := d.ingestParquet(ctx, conn, item.URL, item.TableName, item.NetworkColumn, item.Network); err != nil {
				return fmt.Errorf("worker %d failed to ingest %s: %w", id, item.URL, err)
			}
		}
	}
}

func (d *dataLoader) ingestParquet(ctx context.Context, conn driver.Conn, url, tableName, networkColumn, network string) error {
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
