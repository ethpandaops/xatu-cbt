package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Collector interface for metrics collection
type Collector interface {
	Start(ctx context.Context) error
	Stop() error
	RecordParquetLoad(metric ParquetLoadMetric)
	RecordTestResult(metric TestResultMetric)
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

func (c *collector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startTime = time.Now()
	c.log.Info("metrics collector started")
	return nil
}

func (c *collector) Stop() error {
	c.log.Info("metrics collector stopped")
	return nil
}

func (c *collector) RecordParquetLoad(metric ParquetLoadMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.parquetMetrics = append(c.parquetMetrics, metric)
}

func (c *collector) RecordTestResult(metric TestResultMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.testMetrics = append(c.testMetrics, metric)
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
