// Package testcfg provides test execution configuration.
// This package defines operational parameters for how tests execute
// (timeouts, worker counts, polling intervals) rather than what tests to run.
package testcfg

import "time"

// TestConfig holds test execution operational parameters.
// This configures how tests execute (timeouts, worker counts, polling intervals)
// rather than what tests to run (see testdef.TestDefinition).
type TestConfig struct {
	// Docker configuration
	DockerImage   string
	DockerNetwork string

	// Execution timeouts
	ExecutionTimeout          time.Duration
	TransformationWaitTimeout time.Duration
	PendingModelTimeout       time.Duration

	// Database configuration
	MaxParquetLoadWorkers int
	QueryTimeout          time.Duration

	// Cache configuration
	MaxConcurrentDownloads int
	DefaultHTTPTimeout     time.Duration

	// Polling configuration
	InitialPollInterval    time.Duration
	MaxPollInterval        time.Duration
	AdminTablePollInterval time.Duration
	PollBackoffMultiplier  float64
}

// DefaultTestConfig returns a TestConfig with default values for all test execution parameters.
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		// Docker
		DockerImage:   "ethpandaops/cbt:debian-latest",
		DockerNetwork: "xatu_xatu-net",

		// Execution timeouts
		ExecutionTimeout:          30 * time.Minute,
		TransformationWaitTimeout: 10 * time.Minute,
		PendingModelTimeout:       90 * time.Second,

		// Database
		MaxParquetLoadWorkers: 10,
		QueryTimeout:          5 * time.Minute,

		// Cache
		MaxConcurrentDownloads: 10,
		DefaultHTTPTimeout:     10 * time.Minute,

		// Polling
		InitialPollInterval:    2 * time.Second,
		MaxPollInterval:        10 * time.Second,
		AdminTablePollInterval: 500 * time.Millisecond,
		PollBackoffMultiplier:  1.5,
	}
}
