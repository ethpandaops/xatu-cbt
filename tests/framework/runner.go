package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
)

func main() {
	var (
		pattern        string
		timeout        int
		databasePrefix string
	)

	flag.StringVar(&pattern, "pattern", "*.test.yaml", "Test file pattern")
	flag.IntVar(&timeout, "timeout", 300, "Timeout in seconds")
	flag.StringVar(&databasePrefix, "database-prefix", "", "Database prefix for isolation")
	flag.Parse()

	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	if databasePrefix == "" {
		log.Fatal("--database-prefix flag is required")
	}

	ctx := context.Background()

	if err := runTests(ctx, log, pattern, databasePrefix, timeout); err != nil {
		log.WithError(err).Fatal("Failed to run tests")
	}
}

func runTests(ctx context.Context, log *logrus.Logger, pattern string, databasePrefix string, timeout int) error {
	testFiles, err := filepath.Glob(filepath.Join("models", "**", pattern))
	if err != nil {
		return fmt.Errorf("failed to find test files: %w", err)
	}

	if len(testFiles) == 0 {
		log.WithField("pattern", pattern).Warn("No test files found")
		return nil
	}

	log.WithField("files", len(testFiles)).Info("Found test files")

	dsn := fmt.Sprintf("clickhouse://localhost:9000/%sadmin?compress=true", databasePrefix)
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	startTime := time.Now()
	pollInterval := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for transformations to complete")
		case <-time.After(pollInterval):
			completed, err := checkTransformationStatus(ctx, db, databasePrefix)
			if err != nil {
				log.WithError(err).Warn("Failed to check transformation status")
				continue
			}

			if completed {
				log.WithField("duration", time.Since(startTime)).Info("All transformations completed")
				return nil
			}

			log.WithField("elapsed", time.Since(startTime)).Info("Waiting for transformations to complete")
		}
	}
}

func checkTransformationStatus(ctx context.Context, db *sql.DB, databasePrefix string) (bool, error) {
	// Check if any transformations have been processed
	query := fmt.Sprintf(`
		SELECT 
			database,
			table,
			COUNT(*) as processed_intervals
		FROM %sadmin.cbt
		GROUP BY database, table
	`, databasePrefix)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		// Table might not exist yet
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check transformation status: %w", err)
	}
	defer rows.Close()

	modelCount := 0
	for rows.Next() {
		var database, table string
		var processedIntervals int
		if err := rows.Scan(&database, &table, &processedIntervals); err != nil {
			return false, fmt.Errorf("failed to scan transformation status: %w", err)
		}
		modelCount++
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("rows iteration error: %w", err)
	}

	// Consider transformations complete if we have processed at least one model
	return modelCount > 0, nil
}
