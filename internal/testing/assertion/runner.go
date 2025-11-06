// Package assertion provides SQL assertion execution for validating test results.
package assertion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/testing/testdef"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	defaultWorkers          = 5
	defaultTimeout          = 30 * time.Second
	defaultSyncMaxWait      = 30 * time.Second       // ANTI-FLAKE #10
	defaultSyncPollInterval = 500 * time.Millisecond // ANTI-FLAKE #10
)

var (
	errNoRowsReturned = errors.New("no rows returned")

	// timeFormats contains supported timestamp formats for parsing.
	// Used by compareTimestamps and normalizeTimestamp for consistent handling.
	timeFormats = []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05 -0700 UTC",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
	}
)

// Runner executes SQL assertions.
type Runner interface {
	Start(ctx context.Context) error
	Stop() error
	RunAssertions(ctx context.Context, dbName string, assertions []*testdef.Assertion) (*RunResult, error)
}

// RunResult contains assertion execution results.
type RunResult struct {
	Total    int
	Passed   int
	Failed   int
	Duration time.Duration
	Results  []*Result
}

// Result represents a single assertion result.
type Result struct {
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
	syncMaxWait      time.Duration
	syncPollInterval time.Duration
	log              logrus.FieldLogger

	conn *sql.DB
}

// NewRunner creates a new assertion runner.
func NewRunner(log logrus.FieldLogger, connStr string, workers int, timeout time.Duration) Runner {
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
		syncMaxWait:      defaultSyncMaxWait,
		syncPollInterval: defaultSyncPollInterval,
		log:              log.WithField("component", "assertion_runner"),
	}
}

func (r *runner) Start(ctx context.Context) error {
	r.log.Debug("starting assertion runner")

	conn, err := sql.Open("clickhouse", r.connStr)
	if err != nil {
		return fmt.Errorf("opening clickhouse connection: %w", err)
	}

	if err := conn.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging clickhouse: %w", err)
	}

	r.conn = conn
	r.log.Info("assertion runner started")

	return nil
}

func (r *runner) Stop() error {
	r.log.Debug("stopping assertion runner")

	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			return fmt.Errorf("closing connection: %w", err)
		}
	}

	return nil
}

// RunAssertions executes all assertions for a test.
func (r *runner) RunAssertions(ctx context.Context, dbName string, assertions []*testdef.Assertion) (*RunResult, error) {
	start := time.Now()

	// Execute assertions in parallel with worker pool.
	results := make([]*Result, len(assertions))
	g, gCtx := errgroup.WithContext(ctx)

	sem := make(chan struct{}, r.workers)
	for i, assertion := range assertions {
		i, assertion := i, assertion
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
		"total":    result.Total,
		"passed":   result.Passed,
		"failed":   result.Failed,
		"duration": result.Duration,
	}).Info("assertions complete")

	return result, nil
}

// executeAssertion runs a single assertion with retry logic.
func (r *runner) executeAssertion(ctx context.Context, dbName string, assertion *testdef.Assertion) *Result {
	var (
		start  = time.Now()
		result = &Result{
			Name:     assertion.Name,
			Expected: assertion.Expected,
		}
		// Retry logic for data not ready scenarios
		// Handles race condition where table exists but INSERT hasn't completed
		maxRetries = 3
		retryDelay = 2 * time.Second
	)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			r.log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"attempt":   attempt,
				"max":       maxRetries,
			}).Debug("retrying assertion")

			select {
			case <-ctx.Done():
				result.Error = ctx.Err()
				result.Duration = time.Since(start)
				return result
			case <-time.After(retryDelay):
				// Exponential backoff.
				retryDelay *= 2
			}
		}

		// Execute query with timeout.
		queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
		actual, err := r.queryToMap(queryCtx, dbName, assertion.SQL)
		cancel()

		if err != nil {
			// Only retry on "no rows" errors (data not ready yet).
			if err.Error() == "no rows returned" && attempt < maxRetries {
				r.log.WithField("assertion", assertion.Name).Debug("no rows returned, retrying")

				continue
			}

			result.Error = fmt.Errorf("executing query: %w", err)
			result.Duration = time.Since(start)

			return result
		}

		result.Actual = actual
		result.Passed = r.compareResults(assertion.Expected, actual)

		// If passed, return immediately.
		if result.Passed {
			result.Duration = time.Since(start)

			r.log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"passed":    true,
				"attempts":  attempt + 1,
				"duration":  result.Duration,
			}).Debug("assertion executed")

			return result
		}

		// If failed but not the last attempt, retry.
		if attempt < maxRetries {
			r.log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"expected":  assertion.Expected,
				"actual":    actual,
			}).Debug("assertion failed, will retry")

			continue
		}

		// Last attempt failed
		normalizedExpected := r.normalizeTimestampsInMap(assertion.Expected)
		normalizedActual := r.normalizeTimestampsInMap(actual)
		result.Error = fmt.Errorf("assertion failed: expected %v, got %v", normalizedExpected, normalizedActual) //nolint:err113 // Dynamic error with context needed for debugging

		break
	}

	result.Duration = time.Since(start)

	r.log.WithFields(logrus.Fields{
		"assertion": assertion.Name,
		"passed":    result.Passed,
		"duration":  result.Duration,
	}).Debug("assertion executed")

	return result
}

// queryToMap executes SQL and returns first row as a map.
func (r *runner) queryToMap(
	ctx context.Context,
	dbName, sqlQuery string,
) (map[string]interface{}, error) {
	conn, err := r.conn.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting connection from pool: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Set database on this specific connection.
	if _, execErr := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbName)); execErr != nil {
		return nil, fmt.Errorf("setting database: %w", execErr)
	}

	rows, err := conn.QueryContext(ctx, sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns: %w", err)
	}

	if !rows.Next() {
		return nil, errNoRowsReturned
	}

	// Create slice of interface{} to hold values.
	var (
		values    = make([]interface{}, len(columns))
		valuePtrs = make([]interface{}, len(columns))
	)

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	// Build result map.
	result := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		val := values[i]

		// Convert []byte to string for comparison.
		if b, ok := val.([]byte); ok {
			result[col] = string(b)
		} else {
			result[col] = val
		}
	}

	return result, nil
}

// compareResults performs deep equality check of expected vs actual.
func (r *runner) compareResults(
	expected, actual map[string]interface{},
) bool {
	if len(expected) != len(actual) {
		return false
	}

	for key, expectedVal := range expected {
		actualVal, ok := actual[key]
		if !ok {
			return false
		}

		// Try to parse as timestamps first.
		if r.compareTimestamps(expectedVal, actualVal) {
			continue
		}

		// Convert both to strings for comparison to handle type differences.
		var (
			expectedStr = fmt.Sprintf("%v", expectedVal)
			actualStr   = fmt.Sprintf("%v", actualVal)
		)

		if expectedStr != actualStr {
			// Try direct comparison as fallback.
			if !reflect.DeepEqual(expectedVal, actualVal) {
				return false
			}
		}
	}

	return true
}

// parseTimestamp attempts to parse a value as a timestamp using known formats.
// Returns the parsed time and true if successful, otherwise zero time and false.
func (r *runner) parseTimestamp(val interface{}) (time.Time, bool) {
	valStr := fmt.Sprintf("%v", val)
	for _, format := range timeFormats {
		if t, err := time.Parse(format, valStr); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// compareTimestamps attempts to parse and compare values as timestamps.
func (r *runner) compareTimestamps(expected, actual interface{}) bool {
	expectedTime, expectedOk := r.parseTimestamp(expected)
	actualTime, actualOk := r.parseTimestamp(actual)
	return expectedOk && actualOk && expectedTime.Equal(actualTime)
}

// normalizeTimestampsInMap converts all timestamp values in a map to RFC3339 format for consistent display
func (r *runner) normalizeTimestampsInMap(m map[string]interface{}) map[string]interface{} {
	normalized := make(map[string]interface{})

	for key, val := range m {
		normalized[key] = r.normalizeTimestamp(val)
	}

	return normalized
}

// normalizeTimestamp attempts to parse a value as a timestamp and convert to RFC3339, otherwise returns original.
func (r *runner) normalizeTimestamp(val interface{}) interface{} {
	if t, ok := r.parseTimestamp(val); ok {
		// Convert to RFC3339 for consistent display
		return t.Format(time.RFC3339)
	}
	return val
}
