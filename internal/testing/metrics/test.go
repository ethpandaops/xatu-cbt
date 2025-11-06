package metrics

import "time"

// FailedAssertionDetail captures details about a single failed assertion
type FailedAssertionDetail struct {
	Name     string
	Expected map[string]interface{}
	Actual   map[string]interface{}
	Error    string
}

// TestResultMetric captures metrics about a test execution
type TestResultMetric struct {
	Model             string
	Passed            bool
	Duration          time.Duration
	AssertionsTotal   int
	AssertionsPassed  int
	AssertionsFailed  int
	ErrorMessage      string // empty if passed
	FailedAssertions  []FailedAssertionDetail
	Timestamp         time.Time
}
