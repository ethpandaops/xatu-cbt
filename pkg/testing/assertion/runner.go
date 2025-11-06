package assertion

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/ethpandaops/xatu-cbt/pkg/testing/config"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Runner executes SQL assertions
type Runner interface {
	Start(ctx context.Context) error
	Stop() error
	RunAssertions(ctx context.Context, dbName string, assertions []*config.Assertion) (*RunResult, error)
}

// RunResult contains assertion execution results
type RunResult struct {
	Total    int
	Passed   int
	Failed   int
	Duration time.Duration
	Results  []*AssertionResult
}

// AssertionResult represents a single assertion result
type AssertionResult struct {
	Name     string
	Passed   bool
	Error    error
	Duration time.Duration
	Expected map[string]interface{}
	Actual   map[string]interface{}
}

type runner struct {
	connStr          string
	workers          int
	timeout          time.Duration
	syncMaxWait      time.Duration // ANTI-FLAKE #10: Max time to wait for cluster sync
	syncPollInterval time.Duration // ANTI-FLAKE #10: Polling interval for sync check
	log              logrus.FieldLogger

	conn *sql.DB
}

const (
	defaultWorkers       = 5
	defaultTimeout       = 30 * time.Second
	defaultSyncMaxWait   = 30 * time.Second // ANTI-FLAKE #10
	defaultSyncPollInterval = 500 * time.Millisecond // ANTI-FLAKE #10
)

// NewRunner creates a new assertion runner
func NewRunner(connStr string, workers int, timeout time.Duration, log logrus.FieldLogger) Runner {
	if workers <= 0 {
		workers = defaultWorkers
	}

	if timeout <= 0 {
		timeout = defaultTimeout
	}

	return &runner{
		connStr:          connStr,
		workers:          workers,
		timeout:          timeout,
		syncMaxWait:      defaultSyncMaxWait,      // ANTI-FLAKE #10
		syncPollInterval: defaultSyncPollInterval, // ANTI-FLAKE #10
		log:              log.WithField("component", "assertion_runner"),
	}
}

// Start opens connection pool to ClickHouse
func (r *runner) Start(ctx context.Context) error {
	r.log.Debug("starting assertion runner")

	conn, err := sql.Open("clickhouse", r.connStr)
	if err != nil {
		return fmt.Errorf("opening clickhouse connection: %w", err)
	}

	// Test connection
	if err := conn.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging clickhouse: %w", err)
	}

	r.conn = conn
	r.log.Info("assertion runner started")

	return nil
}

// Stop closes the connection
func (r *runner) Stop() error {
	r.log.Debug("stopping assertion runner")

	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			return fmt.Errorf("closing connection: %w", err)
		}
	}

	return nil
}

// RunAssertions executes all assertions for a test
func (r *runner) RunAssertions(ctx context.Context, dbName string, assertions []*config.Assertion) (*RunResult, error) {
	r.log.WithFields(logrus.Fields{
		"database":   dbName,
		"assertions": len(assertions),
	}).Debug("running assertions")

	start := time.Now()

	// ANTI-FLAKE #10: Wait for cluster replication sync before running assertions
	r.log.Debug("waiting for cluster replication sync")
	if err := r.waitForClusterSync(ctx, dbName); err != nil {
		return nil, fmt.Errorf("waiting for cluster sync: %w", err)
	}
	r.log.Debug("cluster replication sync complete")

	// Execute assertions in parallel with worker pool
	results := make([]*AssertionResult, len(assertions))
	g, gCtx := errgroup.WithContext(ctx)

	sem := make(chan struct{}, r.workers)
	for i, assertion := range assertions {
		i, assertion := i, assertion // Capture loop variables
		g.Go(func() error {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-gCtx.Done():
				return gCtx.Err()
			}

			results[i] = r.executeAssertion(gCtx, dbName, assertion)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Calculate totals
	result := &RunResult{
		Total:    len(results),
		Duration: time.Since(start),
		Results:  results,
	}

	for _, ar := range results {
		if ar.Passed {
			result.Passed++
		} else {
			result.Failed++
		}
	}

	r.log.WithFields(logrus.Fields{
		"database": dbName,
		"total":    result.Total,
		"passed":   result.Passed,
		"failed":   result.Failed,
		"duration": result.Duration,
	}).Info("assertions complete")

	return result, nil
}

// executeAssertion runs a single assertion
func (r *runner) executeAssertion(ctx context.Context, dbName string, assertion *config.Assertion) *AssertionResult {
	r.log.WithField("assertion", assertion.Name).Debug("executing assertion")

	start := time.Now()
	result := &AssertionResult{
		Name:     assertion.Name,
		Expected: assertion.Expected,
	}

	// Execute query with timeout
	queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	actual, err := r.queryToMap(queryCtx, dbName, assertion.SQL)
	if err != nil {
		result.Error = fmt.Errorf("executing query: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	result.Actual = actual
	result.Passed = r.compareResults(assertion.Expected, actual)
	result.Duration = time.Since(start)

	if !result.Passed && result.Error == nil {
		result.Error = fmt.Errorf("assertion failed: expected %v, got %v", assertion.Expected, actual)
	}

	r.log.WithFields(logrus.Fields{
		"assertion": assertion.Name,
		"passed":    result.Passed,
		"duration":  result.Duration,
	}).Debug("assertion executed")

	return result
}

// queryToMap executes SQL and returns first row as a map
func (r *runner) queryToMap(ctx context.Context, dbName, sql string) (map[string]interface{}, error) {
	// ANTI-FLAKE: Get a dedicated connection from pool to ensure USE and query execute on same connection
	// sql.DB is a connection pool - using Conn() ensures session state (USE database) persists
	conn, err := r.conn.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting connection from pool: %w", err)
	}
	defer conn.Close()

	// Set database on this specific connection
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbName)); err != nil {
		return nil, fmt.Errorf("setting database: %w", err)
	}

	// Execute query on same connection that has USE database set
	rows, err := conn.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns: %w", err)
	}

	if !rows.Next() {
		return nil, fmt.Errorf("no rows returned")
	}

	// Create slice of interface{} to hold values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	// Build result map
	result := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		val := values[i]

		// Convert []byte to string for comparison
		if b, ok := val.([]byte); ok {
			result[col] = string(b)
		} else {
			result[col] = val
		}
	}

	return result, nil
}

// compareResults performs deep equality check of expected vs actual
func (r *runner) compareResults(expected, actual map[string]interface{}) bool {
	if len(expected) != len(actual) {
		return false
	}

	for key, expectedVal := range expected {
		actualVal, ok := actual[key]
		if !ok {
			return false
		}

		// Try to parse as timestamps first
		if r.compareTimestamps(expectedVal, actualVal) {
			continue
		}

		// Convert both to strings for comparison to handle type differences
		expectedStr := fmt.Sprintf("%v", expectedVal)
		actualStr := fmt.Sprintf("%v", actualVal)

		if expectedStr != actualStr {
			// Try direct comparison as fallback
			if !reflect.DeepEqual(expectedVal, actualVal) {
				return false
			}
		}
	}

	return true
}

// compareTimestamps attempts to parse and compare values as timestamps
func (r *runner) compareTimestamps(expected, actual interface{}) bool {
	expectedStr := fmt.Sprintf("%v", expected)
	actualStr := fmt.Sprintf("%v", actual)

	// Try common timestamp formats
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05 -0700 UTC",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
	}

	var expectedTime, actualTime time.Time
	var expectedParsed, actualParsed bool

	for _, format := range formats {
		if t, err := time.Parse(format, expectedStr); err == nil {
			expectedTime = t
			expectedParsed = true
			break
		}
	}

	for _, format := range formats {
		if t, err := time.Parse(format, actualStr); err == nil {
			actualTime = t
			actualParsed = true
			break
		}
	}

	// If both parsed as timestamps, compare them
	if expectedParsed && actualParsed {
		return expectedTime.Equal(actualTime)
	}

	return false
}

// waitForClusterSync ensures all data is replicated across cluster nodes
// ANTI-FLAKE #10: This prevents timing issues where assertions run before data is fully synced
func (r *runner) waitForClusterSync(ctx context.Context, dbName string) error {
	deadline := time.Now().Add(r.syncMaxWait)
	ticker := time.NewTicker(r.syncPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check if replication queue is empty for our database
			query := `
				SELECT count() as pending
				FROM system.replication_queue
				WHERE database = ?
			`

			var pending int64
			err := r.conn.QueryRowContext(ctx, query, dbName).Scan(&pending)
			if err != nil {
				return fmt.Errorf("querying replication queue: %w", err)
			}

			// If no pending operations, cluster is synced
			if pending == 0 {
				return nil
			}

			// Check timeout
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for cluster sync: %d operations still pending", pending)
			}

			r.log.WithField("pending", pending).Debug("waiting for cluster sync")
		}
	}
}
