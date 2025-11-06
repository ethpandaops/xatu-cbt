package metrics

import "time"

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
