// Package metrics provides test execution metrics collection and aggregation.
package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// ParquetLoadSource indicates where parquet data was loaded from
type ParquetLoadSource string

const (
	// SourceCache defines if parquet was loaded via cache.
	SourceCache ParquetLoadSource = "cache"
	// SourceS3 defines if parquet was loaded via s3.
	SourceS3 ParquetLoadSource = "s3"
)

// ParquetLoadMetric captures metrics about loading a parquet file
type ParquetLoadMetric struct {
	Table     string
	Source    ParquetLoadSource // cache or s3
	SizeBytes int64
	Duration  time.Duration
	Timestamp time.Time
}

// FailedAssertionDetail captures details about a single failed assertion
type FailedAssertionDetail struct {
	Name     string
	Expected map[string]interface{}
	Actual   map[string]interface{}
	Error    string
}

// TestResultMetric captures metrics about a test execution
type TestResultMetric struct {
	Model            string
	Passed           bool
	Duration         time.Duration
	AssertionsTotal  int
	AssertionsPassed int
	AssertionsFailed int
	ErrorMessage     string // empty if passed
	FailedAssertions []FailedAssertionDetail
	Timestamp        time.Time
}

// SummaryMetric provides aggregate statistics across all operations
type SummaryMetric struct {
	TotalDuration time.Duration
	TotalTests    int
	PassedTests   int
	FailedTests   int
	CacheHits     int
	CacheMisses   int
	CacheHitRate  float64 // percentage
	TotalDataSize int64   // bytes
}

// Collector interface for metrics collection
type Collector interface {
	Start(ctx context.Context) error
	Stop() error
	RecordParquetLoad(metric ParquetLoadMetric)
	RecordTestResult(metric *TestResultMetric)
	GetParquetMetrics() []ParquetLoadMetric
	GetTestMetrics() []TestResultMetric
	GetSummary() SummaryMetric
}

// collector implements Collector interface
type collector struct {
	log            logrus.FieldLogger
	mu             sync.RWMutex
	parquetMetrics []ParquetLoadMetric
	testMetrics    []TestResultMetric
	startTime      time.Time
}

// NewCollector creates a new metrics collector
// Returns Collector interface, not *collector struct (per ethPandaOps standards)
func NewCollector(log logrus.FieldLogger) Collector {
	return &collector{
		log:            log.WithField("component", "metrics_collector"),
		parquetMetrics: make([]ParquetLoadMetric, 0, 100), // capacity hint
		testMetrics:    make([]TestResultMetric, 0, 50),   // capacity hint
	}
}

func (c *collector) Start(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startTime = time.Now()

	c.log.Debug("metrics collector started")

	return nil
}

func (c *collector) Stop() error {
	c.log.Debug("metrics collector stopped")

	return nil
}

func (c *collector) RecordParquetLoad(metric ParquetLoadMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.parquetMetrics = append(c.parquetMetrics, metric)
}

func (c *collector) RecordTestResult(metric *TestResultMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.testMetrics = append(c.testMetrics, *metric)
}

func (c *collector) GetParquetMetrics() []ParquetLoadMetric {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Return copy to avoid race conditions
	result := make([]ParquetLoadMetric, len(c.parquetMetrics))
	copy(result, c.parquetMetrics)
	return result
}

func (c *collector) GetTestMetrics() []TestResultMetric {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]TestResultMetric, len(c.testMetrics))
	copy(result, c.testMetrics)
	return result
}

func (c *collector) GetSummary() SummaryMetric {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalDuration := time.Since(c.startTime)
	cacheHits := 0
	cacheMisses := 0
	totalSize := int64(0)

	for _, pm := range c.parquetMetrics {
		if pm.Source == SourceCache {
			cacheHits++
		} else {
			cacheMisses++
		}
		totalSize += pm.SizeBytes
	}

	passed := 0
	failed := 0
	for _, tm := range c.testMetrics {
		if tm.Passed {
			passed++
		} else {
			failed++
		}
	}

	cacheHitRate := 0.0
	if cacheHits+cacheMisses > 0 {
		cacheHitRate = float64(cacheHits) / float64(cacheHits+cacheMisses) * 100.0
	}

	return SummaryMetric{
		TotalDuration: totalDuration,
		TotalTests:    len(c.testMetrics),
		PassedTests:   passed,
		FailedTests:   failed,
		CacheHits:     cacheHits,
		CacheMisses:   cacheMisses,
		CacheHitRate:  cacheHitRate,
		TotalDataSize: totalSize,
	}
}

// Compile-time interface compliance check
var _ Collector = (*collector)(nil)
