// Package test provides testing utilities for xatu-cbt
package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/savid/xatu-cbt/pkg/clickhouse"
	"github.com/savid/xatu-cbt/pkg/config"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// Common errors for assertions
var (
	ErrMultipleRowsReturned = errors.New("query returned multiple rows, expected one")
	ErrColumnNotFound       = errors.New("expected column not found in results")
)

// AssertionRunner executes SQL assertions against ClickHouse
type AssertionRunner interface {
	RunAssertions(ctx context.Context, assertionsDir string) (bool, error)
}

type assertionRunner struct {
	log logrus.FieldLogger
}

// Assertion defines a SQL query and expected results
type Assertion struct {
	Name     string            `yaml:"name"`
	SQL      string            `yaml:"sql"`
	Expected map[string]string `yaml:"expected"`
}

// UnmarshalYAML custom unmarshaler to convert all expected values to strings
func (a *Assertion) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Use an auxiliary struct to avoid recursion
	type aux struct {
		Name     string                 `yaml:"name"`
		SQL      string                 `yaml:"sql"`
		Expected map[string]interface{} `yaml:"expected"`
	}

	var tmp aux
	if err := unmarshal(&tmp); err != nil {
		return err
	}

	a.Name = tmp.Name
	a.SQL = tmp.SQL
	a.Expected = make(map[string]string)

	// Convert all expected values to strings
	for k, v := range tmp.Expected {
		a.Expected[k] = fmt.Sprintf("%v", v)
	}

	return nil
}

// NewAssertionRunner creates a new assertion runner instance
func NewAssertionRunner(log logrus.FieldLogger) AssertionRunner {
	return &assertionRunner{
		log: log.WithField("component", "assertion_runner"),
	}
}

// AssertionStatus tracks the status of a single assertion
type AssertionStatus struct {
	Name   string
	File   string
	Passed bool
}

func (a *assertionRunner) RunAssertions(ctx context.Context, assertionsDir string) (bool, error) {
	files, err := filepath.Glob(filepath.Join(assertionsDir, "*.yaml"))
	if err != nil {
		return false, fmt.Errorf("failed to list assertion files: %w", err)
	}

	if len(files) == 0 {
		a.log.Debug("No assertion files found")
		return true, nil
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return false, fmt.Errorf("failed to load config: %w", err)
	}

	// Collect all assertions for tracking
	totalAssertions := 0
	for _, file := range files {
		data, err := os.ReadFile(file) //nolint:gosec // file path is from controlled source
		if err != nil {
			continue
		}
		var assertions []Assertion
		if err := yaml.Unmarshal(data, &assertions); err != nil {
			continue
		}
		totalAssertions += len(assertions)
	}

	// Run assertions in parallel with a worker pool
	const maxWorkers = 5 // Limit concurrent database connections
	results := a.runAssertionsParallel(ctx, cfg, files, maxWorkers)

	// Calculate totals from results
	totalPassed := 0
	totalFailed := 0
	totalPending := 0
	for _, status := range results {
		totalPassed += status.Passed
		totalFailed += status.Failed
		totalPending += status.Pending
	}

	// Log overall summary at info level
	summaryMsg := "📊 Assertions check summary"

	// Adjust message based on status but keep at info level
	if totalFailed > 0 { //nolint:gocritic // if-else chain is more readable for these conditions
		summaryMsg = "⚠️  Assertions check summary - SQL ERRORS"
	} else if totalPending > 0 {
		summaryMsg = "⏳ Assertions check summary - WAITING FOR DATA"
	} else if totalPassed == totalAssertions {
		summaryMsg = "✅ Assertions check summary - ALL PASSED"
	}

	a.log.WithFields(logrus.Fields{
		"total":   totalAssertions,
		"passed":  totalPassed,
		"failed":  totalFailed,
		"pending": totalPending,
	}).Info(summaryMsg)

	// Only return success if all passed (no failures and no pending)
	return totalFailed == 0 && totalPending == 0 && totalPassed == totalAssertions, nil
}

// runAssertionsParallel runs assertion files in parallel using a worker pool
func (a *assertionRunner) runAssertionsParallel(ctx context.Context, cfg *config.Config, files []string, maxWorkers int) map[string]AssertionFileStatus {
	var wg sync.WaitGroup
	fileChan := make(chan string, len(files))
	resultChan := make(chan struct {
		file   string
		status AssertionFileStatus
	}, len(files))

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()

			// Each worker gets its own database connection
			conn, err := clickhouse.Connect(cfg)
			if err != nil {
				a.log.WithError(err).Error("Worker failed to connect to clickhouse")
				return
			}
			defer func() {
				if err := conn.Close(); err != nil {
					a.log.WithError(err).Debug("Worker failed to close connection")
				}
			}()

			// Process files from the channel
			for file := range fileChan {
				select {
				case <-ctx.Done():
					return
				default:
					_, status := a.runAssertionFileWithStatus(ctx, conn, file)
					resultChan <- struct {
						file   string
						status AssertionFileStatus
					}{file: filepath.Base(file), status: status}
				}
			}
		}(i)
	}

	// Queue all files
	for _, file := range files {
		fileChan <- file
	}
	close(fileChan)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	results := make(map[string]AssertionFileStatus)
	for result := range resultChan {
		results[result.file] = result.status
	}

	return results
}

// AssertionFileStatus holds the status of assertions in a file
type AssertionFileStatus struct {
	Passed  int
	Failed  int // SQL errors or query failures
	Pending int // Value mismatches (not matching expected yet)
	Total   int
}

func (a *assertionRunner) runAssertionFileWithStatus(ctx context.Context, conn driver.Conn, file string) (allPassed bool, status AssertionFileStatus) {
	data, err := os.ReadFile(file) //nolint:gosec // file path is from controlled source
	if err != nil {
		a.log.WithError(err).Error("Failed to read assertion file")
		return false, status
	}

	var assertions []Assertion
	if err := yaml.Unmarshal(data, &assertions); err != nil {
		a.log.WithError(err).Error("Failed to parse assertion file")
		return false, status
	}

	status.Total = len(assertions)

	for _, assertion := range assertions {
		passed, err := a.runAssertion(ctx, conn, file, assertion)
		if err != nil {
			a.log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"error":     err,
			}).Error("Failed to run assertion (SQL error)")
			// SQL error - this is a failure
			status.Failed++
			continue
		}

		if passed {
			status.Passed++
		} else {
			// Value mismatch - this is pending (data not ready yet)
			status.Pending++
		}
	}

	return status.Failed == 0 && status.Pending == 0, status
}

func (a *assertionRunner) runAssertion(ctx context.Context, conn driver.Conn, file string, assertion Assertion) (bool, error) { //nolint:gocyclo // Complex function handling many database types
	// Get the network name from config to use as database
	cfg, err := config.Load()
	if err != nil {
		return false, fmt.Errorf("failed to load config: %w", err)
	}

	// Create a separate context for database operations with a 30-second timeout
	// This prevents the overall test timeout from affecting individual queries
	// But still respects cancellation from the parent context
	dbCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Set the database context to the network database
	if execErr := conn.Exec(dbCtx, fmt.Sprintf("USE `%s`", cfg.Network)); execErr != nil {
		// If network database doesn't exist, try default
		a.log.WithField("network", cfg.Network).Debug("Network database not found, using default")
		if fallbackErr := conn.Exec(dbCtx, "USE default"); fallbackErr != nil {
			return false, fmt.Errorf("failed to set database context: %w", fallbackErr)
		}
	}

	rows, err := conn.Query(dbCtx, assertion.SQL)
	if err != nil { //nolint:nestif // Necessary for proper error categorization and debug logging
		// Check if it's a table not found error
		errStr := err.Error()
		if strings.Contains(errStr, "Unknown table") || strings.Contains(errStr, "doesn't exist") {
			a.log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"error":     err,
			}).Debug("Table doesn't exist yet, will retry")
			return false, nil // Return false but no error so we keep retrying
		}
		return false, fmt.Errorf("failed to execute query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			a.log.WithError(err).Debug("Failed to close rows")
		}
	}()

	if !rows.Next() {
		a.log.WithFields(logrus.Fields{
			"assertion": assertion.Name,
			"sql":       assertion.SQL,
		}).Debug("Query returned no rows")
		return false, nil // Return false but no error so we keep retrying
	}

	// Get column names and types
	columns := rows.Columns()
	columnTypes := rows.ColumnTypes()

	// Create appropriate scan variables based on column types
	scanArgs := make([]interface{}, len(columns))
	values := make([]string, len(columns))

	for i, colType := range columnTypes {
		// Based on the ClickHouse type, create appropriate scan variable
		// We'll handle the most common types and scan them properly
		switch colType.DatabaseTypeName() {
		case "String", "FixedString", "UUID", "Enum8", "Enum16":
			scanArgs[i] = new(string)
		case "UInt8":
			scanArgs[i] = new(uint8)
		case "UInt16":
			scanArgs[i] = new(uint16)
		case "UInt32":
			scanArgs[i] = new(uint32)
		case "UInt64":
			scanArgs[i] = new(uint64)
		case "Int8":
			scanArgs[i] = new(int8)
		case "Int16":
			scanArgs[i] = new(int16)
		case "Int32":
			scanArgs[i] = new(int32)
		case "Int64":
			scanArgs[i] = new(int64)
		case "Float32":
			scanArgs[i] = new(float32)
		case "Float64":
			scanArgs[i] = new(float64)
		case "Bool":
			scanArgs[i] = new(bool)
		case "Date", "Date32", "DateTime", "DateTime64":
			scanArgs[i] = new(time.Time)
		default:
			// For any unknown type, try string
			scanArgs[i] = new(string)
		}
	}

	// Scan the row
	if err := rows.Scan(scanArgs...); err != nil {
		return false, fmt.Errorf("failed to scan row: %w", err)
	}

	// Convert all scanned values to strings for comparison
	for i, v := range scanArgs {
		switch val := v.(type) {
		case *string:
			values[i] = *val
		case *uint8:
			values[i] = fmt.Sprintf("%d", *val)
		case *uint16:
			values[i] = fmt.Sprintf("%d", *val)
		case *uint32:
			values[i] = fmt.Sprintf("%d", *val)
		case *uint64:
			values[i] = fmt.Sprintf("%d", *val)
		case *int8:
			values[i] = fmt.Sprintf("%d", *val)
		case *int16:
			values[i] = fmt.Sprintf("%d", *val)
		case *int32:
			values[i] = fmt.Sprintf("%d", *val)
		case *int64:
			values[i] = fmt.Sprintf("%d", *val)
		case *float32:
			values[i] = fmt.Sprintf("%g", *val)
		case *float64:
			values[i] = fmt.Sprintf("%g", *val)
		case *bool:
			values[i] = fmt.Sprintf("%t", *val)
		case *time.Time:
			values[i] = val.Format(time.RFC3339)
		default:
			values[i] = fmt.Sprintf("%v", v)
		}
	}

	// Check if there are more rows (we expect only one)
	if rows.Next() {
		return false, ErrMultipleRowsReturned
	}

	// Compare with expected values
	for i, col := range columns {
		expectedVal, exists := assertion.Expected[col]
		if !exists {
			continue // Skip columns not in expected
		}

		actualVal := values[i]

		// Both values are already strings, just compare
		if actualVal != expectedVal {
			a.log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"column":    col,
				"expected":  expectedVal,
				"actual":    actualVal,
				"name":      assertion.Name,
				"file":      file,
			}).Debug("Value mismatch detected")
			return false, nil
		}
	}

	// Check that all expected columns were present
	for expectedCol := range assertion.Expected {
		found := false
		for _, col := range columns {
			if col == expectedCol {
				found = true
				break
			}
		}
		if !found {
			return false, fmt.Errorf("%w: %s", ErrColumnNotFound, expectedCol)
		}
	}

	return true, nil
}
