// Package testing provides test execution configuration.
// This package defines operational parameters for how tests execute
// (timeouts, worker counts, polling intervals) rather than what tests to run.
package testing

import (
	"os"
	"strings"
	"time"
)

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

	// CBT container configuration
	CBTConcurrency int // Max concurrent CBT Docker containers

	// Cache configuration
	MaxConcurrentDownloads int
	DefaultHTTPTimeout     time.Duration

	// Polling configuration
	InitialPollInterval    time.Duration
	MaxPollInterval        time.Duration
	AdminTablePollInterval time.Duration
	PollBackoffMultiplier  float64

	// Assertion runner configuration
	AssertionWorkers    int
	AssertionTimeout    time.Duration
	AssertionMaxRetries int
	AssertionRetryDelay time.Duration

	// Safety configuration
	// SafeHostnames is a whitelist of ClickHouse hostnames that are allowed for destructive operations.
	// If a connection is made to a ClickHouse instance whose hostname is not in this list,
	// all TRUNCATE, DROP, and DELETE operations will be blocked to prevent accidental data loss.
	SafeHostnames []string
}

// DefaultTestConfig returns a TestConfig with default values for all test execution parameters.
func DefaultTestConfig() *TestConfig {
	// Read safe hostnames from environment variable, fall back to defaults
	safeHostnames := getSafeHostnames()

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

		// CBT containers
		CBTConcurrency: 5, // Reduced to limit ClickHouse contention under concurrent load

		// Cache
		MaxConcurrentDownloads: 10,
		DefaultHTTPTimeout:     10 * time.Minute,

		// Polling
		InitialPollInterval:    2 * time.Second,
		MaxPollInterval:        10 * time.Second,
		AdminTablePollInterval: 500 * time.Millisecond,
		PollBackoffMultiplier:  1.5,

		// Assertion runner
		AssertionWorkers:    5,
		AssertionTimeout:    30 * time.Second,
		AssertionMaxRetries: 3,
		AssertionRetryDelay: 2 * time.Second,

		// Safety - read from env var or use defaults
		SafeHostnames: safeHostnames,
	}
}

// getSafeHostnames reads safe hostnames from XATU_CBT_SAFE_HOSTS env var or returns defaults.
func getSafeHostnames() []string {
	envHosts := os.Getenv("XATU_CBT_SAFE_HOSTS")
	if envHosts != "" {
		return parseSafeHostnames(envHosts)
	}

	// Default whitelist for local development and test containers
	return []string{}
}

// parseSafeHostnames parses a comma-separated list of hostnames.
// Handles both quoted and unquoted values from environment variables.
func parseSafeHostnames(s string) []string {
	if s == "" {
		return []string{}
	}

	// Remove surrounding quotes if present (handles both " and ')
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			s = s[1 : len(s)-1]
		}
	}

	parts := strings.Split(s, ",")
	hostnames := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			hostnames = append(hostnames, trimmed)
		}
	}

	return hostnames
}
