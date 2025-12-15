// Package assertion provides SQL assertion execution for validating test results.
package assertion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/testing/testdef"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	defaultWorkers    = 5
	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 3
	defaultRetryDelay = 2 * time.Second
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
	RunAssertions(ctx context.Context, model, dbName string, assertions []*testdef.Assertion) (*RunResult, error)
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
	connStr    string
	workers    int
	timeout    time.Duration
	maxRetries int
	retryDelay time.Duration
	log        logrus.FieldLogger

	conn *sql.DB
}

// NewRunner creates a new assertion runner.
func NewRunner(log logrus.FieldLogger, connStr string, workers int, timeout time.Duration, maxRetries int, retryDelay time.Duration) Runner {
	if workers <= 0 {
		workers = defaultWorkers
	}

	if timeout <= 0 {
		timeout = defaultTimeout
	}

	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}

	if retryDelay <= 0 {
		retryDelay = defaultRetryDelay
	}

	return &runner{
		connStr:    connStr,
		workers:    workers,
		timeout:    timeout,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
		log:        log.WithField("component", "assertion_runner"),
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
func (r *runner) RunAssertions(ctx context.Context, model, dbName string, assertions []*testdef.Assertion) (*RunResult, error) {
	start := time.Now()

	// Create a scoped logger with the model name
	log := r.log.WithField("model", model)

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

			results[i] = r.executeAssertion(gCtx, log, dbName, assertion)
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

	log.WithFields(logrus.Fields{
		"total":    result.Total,
		"passed":   result.Passed,
		"failed":   result.Failed,
		"duration": result.Duration,
	}).Info("assertions complete")

	return result, nil
}

// executeAssertion runs a single assertion with retry logic.
func (r *runner) executeAssertion(ctx context.Context, log logrus.FieldLogger, dbName string, assertion *testdef.Assertion) *Result {
	var (
		start  = time.Now()
		result = &Result{
			Name:     assertion.Name,
			Expected: assertion.Expected,
		}
	)

	// Retry logic for data not ready scenarios
	// Handles race condition where table exists but INSERT hasn't completed
	currentRetryDelay := r.retryDelay
	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if attempt > 0 {
			log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"attempt":   attempt,
				"max":       r.maxRetries,
			}).Debug("retrying assertion")

			select {
			case <-ctx.Done():
				result.Error = ctx.Err()
				result.Duration = time.Since(start)
				return result
			case <-time.After(currentRetryDelay):
				// Exponential backoff.
				currentRetryDelay *= 2
			}
		}

		// Substitute variables in SQL:
		// - {database} -> actual test database name (e.g., ext_xxx or cbt_xxx)
		// - cluster('{remote_cluster}', default.TABLE) -> TABLE (since we USE dbName)
		// - default. -> empty (since we USE dbName, tables are in current db context)
		query := strings.ReplaceAll(assertion.SQL, "{database}", dbName)

		// Handle cluster() wrapper - must remove BOTH the prefix AND trailing ) together
		// Pattern: cluster('{remote_cluster}', default.TABLE) -> TABLE
		const clusterPrefix = "cluster('{remote_cluster}', default."
		for strings.Contains(query, clusterPrefix) {
			startIdx := strings.Index(query, clusterPrefix)
			// Find the matching closing ) after the table name
			afterPrefix := startIdx + len(clusterPrefix)
			closeIdx := strings.Index(query[afterPrefix:], ")")
			if closeIdx >= 0 {
				// Remove the cluster() wrapper entirely (prefix and closing paren)
				query = query[:startIdx] + query[afterPrefix:afterPrefix+closeIdx] + query[afterPrefix+closeIdx+1:]
			} else {
				break // Malformed, stop processing
			}
		}

		query = strings.ReplaceAll(query, "{remote_cluster}", dbName) // Fallback for other uses
		query = strings.ReplaceAll(query, "default.", "")             // Remove default. prefix

		// Execute query with timeout.
		queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
		actual, err := r.queryToMap(queryCtx, dbName, query)
		cancel()

		if err != nil {
			// Only retry on "no rows" errors (data not ready yet).
			if err.Error() == "no rows returned" && attempt < r.maxRetries {
				log.WithField("assertion", assertion.Name).Debug("no rows returned, retrying")

				continue
			}

			result.Error = fmt.Errorf("executing query: %w", err)
			result.Duration = time.Since(start)

			return result
		}

		result.Actual = actual

		// Evaluate assertion based on format (exact match vs typed checks)
		if len(assertion.Expected) > 0 {
			// Exact match comparison
			result.Passed = r.compareResults(assertion.Expected, actual)
		} else if len(assertion.Assertions) > 0 {
			// Typed checks comparison
			passed, checkErr := r.evaluateTypedChecks(assertion.Assertions, actual)
			result.Passed = passed
			if checkErr != nil && !passed {
				result.Error = checkErr
			}
		}

		// If passed, return immediately.
		if result.Passed {
			result.Duration = time.Since(start)

			log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"passed":    true,
				"attempts":  attempt + 1,
				"duration":  result.Duration,
			}).Debug("assertion executed")

			return result
		}

		// If failed but not the last attempt, retry.
		if attempt < r.maxRetries {
			log.WithFields(logrus.Fields{
				"assertion": assertion.Name,
				"expected":  assertion.Expected,
				"actual":    actual,
			}).Debug("assertion failed, will retry")

			continue
		}

		// Last attempt failed - format error message based on assertion type
		if result.Error == nil {
			// Only generate error if not already set by evaluateTypedChecks
			normalizedExpected := r.normalizeTimestampsInMap(assertion.Expected)
			normalizedActual := r.normalizeTimestampsInMap(actual)
			result.Error = fmt.Errorf("assertion failed: expected %v, got %v", normalizedExpected, normalizedActual) //nolint:err113 // Dynamic error with context needed for debugging
		}

		break
	}

	result.Duration = time.Since(start)

	log.WithFields(logrus.Fields{
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

// evaluateTypedChecks evaluates typed assertion checks against actual query results
func (r *runner) evaluateTypedChecks(checks []*testdef.TypedCheck, actual map[string]interface{}) (bool, error) {
	var failedChecks []string

	for _, check := range checks {
		actualVal, exists := actual[check.Column]
		if !exists {
			failedChecks = append(failedChecks, fmt.Sprintf("%s: column not found in results", check.Column))
			continue
		}

		passed, err := r.evaluateComparison(check.Type, actualVal, check.Value)
		if err != nil {
			return false, fmt.Errorf("evaluating check for column %s: %w", check.Column, err)
		}

		if !passed {
			failedChecks = append(failedChecks, fmt.Sprintf("%s %s %v (actual: %v)", check.Column, check.Type, check.Value, actualVal))
		}
	}

	if len(failedChecks) > 0 {
		return false, fmt.Errorf("failed checks: %v", failedChecks) //nolint:err113 // Dynamic error with context needed for debugging
	}

	return true, nil
}

// evaluateComparison performs comparison based on type
// Supports numeric and timestamp comparisons
//
//nolint:gocyclo // switch statement throwing it off.
func (r *runner) evaluateComparison(comparisonType string, actual, expected interface{}) (bool, error) {
	// Try timestamp comparison first
	actualTime, actualIsTime := r.parseTimestamp(actual)
	expectedTime, expectedIsTime := r.parseTimestamp(expected)

	if actualIsTime && expectedIsTime {
		return r.compareTimestampsWithOp(comparisonType, actualTime, expectedTime)
	}

	// Convert to float64 for numeric comparisons
	actualFloat, actualIsNumeric := toFloat64(actual)
	expectedFloat, expectedIsNumeric := toFloat64(expected)

	switch comparisonType {
	case "equals", "equal":
		if actualIsNumeric && expectedIsNumeric {
			return actualFloat == expectedFloat, nil
		}
		return fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected), nil

	case "not_equals", "not_equal":
		if actualIsNumeric && expectedIsNumeric {
			return actualFloat != expectedFloat, nil
		}
		return fmt.Sprintf("%v", actual) != fmt.Sprintf("%v", expected), nil

	case "greater_than", "gt":
		if !actualIsNumeric || !expectedIsNumeric {
			return false, fmt.Errorf("greater_than requires numeric or timestamp values, got actual=%T expected=%T", actual, expected) //nolint:err113 // Dynamic error with type info
		}
		return actualFloat > expectedFloat, nil

	case "greater_than_or_equal", "gte":
		if !actualIsNumeric || !expectedIsNumeric {
			return false, fmt.Errorf("greater_than_or_equal requires numeric or timestamp values, got actual=%T expected=%T", actual, expected) //nolint:err113 // Dynamic error with type info
		}
		return actualFloat >= expectedFloat, nil

	case "less_than", "lt":
		if !actualIsNumeric || !expectedIsNumeric {
			return false, fmt.Errorf("less_than requires numeric or timestamp values, got actual=%T expected=%T", actual, expected) //nolint:err113 // Dynamic error with type info
		}
		return actualFloat < expectedFloat, nil

	case "less_than_or_equal", "lte":
		if !actualIsNumeric || !expectedIsNumeric {
			return false, fmt.Errorf("less_than_or_equal requires numeric or timestamp values, got actual=%T expected=%T", actual, expected) //nolint:err113 // Dynamic error with type info
		}
		return actualFloat <= expectedFloat, nil

	default:
		return false, fmt.Errorf("unknown comparison type: %s", comparisonType) //nolint:err113 // Dynamic error with comparison type
	}
}

// compareTimestampsWithOp compares two timestamps using the specified operator
func (r *runner) compareTimestampsWithOp(comparisonType string, actual, expected time.Time) (bool, error) {
	switch comparisonType {
	case "equals", "equal":
		return actual.Equal(expected), nil
	case "not_equals", "not_equal":
		return !actual.Equal(expected), nil
	case "greater_than", "gt":
		return actual.After(expected), nil
	case "greater_than_or_equal", "gte":
		return actual.After(expected) || actual.Equal(expected), nil
	case "less_than", "lt":
		return actual.Before(expected), nil
	case "less_than_or_equal", "lte":
		return actual.Before(expected) || actual.Equal(expected), nil
	default:
		return false, fmt.Errorf("unknown comparison type for timestamps: %s", comparisonType) //nolint:err113 // Dynamic error with comparison type
	}
}

// toFloat64 attempts to convert a value to float64 for numeric comparisons
func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
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
