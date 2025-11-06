package table

import (
	"fmt"

	"github.com/ethpandaops/xatu-cbt/internal/testing/format"
	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/sirupsen/logrus"
)

// SummaryFormatter formats summary statistics as a table
type SummaryFormatter struct {
	log      logrus.FieldLogger
	renderer *Renderer
}

// NewSummaryFormatter creates a new summary table formatter
func NewSummaryFormatter(log logrus.FieldLogger, renderer *Renderer) *SummaryFormatter {
	return &SummaryFormatter{
		log:      log.WithField("component", "table.summary_formatter"),
		renderer: renderer,
	}
}

// Format converts summary metrics into a formatted table string
func (f *SummaryFormatter) Format(summary metrics.SummaryMetric) string {
	passRate := 0.0
	if summary.TotalTests > 0 {
		passRate = float64(summary.PassedTests) / float64(summary.TotalTests) * 100.0
	}

	headers := []string{"Metric", "Value"}
	rows := [][]string{
		{"Total Tests", fmt.Sprintf("%d", summary.TotalTests)},
		{"Passed", fmt.Sprintf("%d (%.1f%%)", summary.PassedTests, passRate)},
		{"Failed", fmt.Sprintf("%d (%.1f%%)", summary.FailedTests, 100.0-passRate)},
		{"Total Duration", format.Duration(summary.TotalDuration)},
		{"Cache Hit Rate", fmt.Sprintf("%.1f%% (%d/%d)",
			summary.CacheHitRate,
			summary.CacheHits,
			summary.CacheHits+summary.CacheMisses)},
		{"Total Data Loaded", format.Bytes(summary.TotalDataSize)},
	}

	return "\n▸ Summary\n\n" + f.renderer.RenderToString(headers, rows)
}
