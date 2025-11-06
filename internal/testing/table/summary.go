package table

import (
	"fmt"

	"github.com/ethpandaops/xatu-cbt/internal/testing/format"
	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/sirupsen/logrus"
)

// SummaryFormatter formats summary statistics as a table.
type SummaryFormatter struct {
	log      logrus.FieldLogger
	renderer *Renderer
	colors   *ColorHelper
}

// NewSummaryFormatter creates a new summary table formatter.
func NewSummaryFormatter(log logrus.FieldLogger, renderer *Renderer) *SummaryFormatter {
	return &SummaryFormatter{
		log:      log.WithField("component", "table.summary_formatter"),
		renderer: renderer,
		colors:   NewColorHelper(),
	}
}

// Format converts summary metrics into a formatted table string.
func (f *SummaryFormatter) Format(summary metrics.SummaryMetric) string {
	var passRate float64
	if summary.TotalTests > 0 {
		passRate = float64(summary.PassedTests) / float64(summary.TotalTests) * 100.0
	}

	// Format values with colors
	passedValue := fmt.Sprintf("%d (%s)", summary.PassedTests, f.colors.FormatPercentage(passRate))
	if summary.PassedTests == summary.TotalTests {
		passedValue = f.colors.Success(fmt.Sprintf("%d (%.1f%%)", summary.PassedTests, passRate))
	}

	failedValue := fmt.Sprintf("%d (%.1f%%)", summary.FailedTests, 100.0-passRate)
	if summary.FailedTests > 0 {
		failedValue = f.colors.Failure(fmt.Sprintf("%d (%.1f%%)", summary.FailedTests, 100.0-passRate))
	} else {
		failedValue = f.colors.Success(failedValue)
	}

	cacheValue := fmt.Sprintf(
		"%.1f%% (%d/%d)",
		summary.CacheHitRate,
		summary.CacheHits,
		summary.CacheHits+summary.CacheMisses,
	)

	switch {
	case summary.CacheHitRate == 100.0:
		cacheValue = f.colors.Success(cacheValue)
	case summary.CacheHitRate >= 50.0:
		cacheValue = f.colors.Warning(cacheValue)
	default:
		cacheValue = f.colors.Muted(cacheValue)
	}

	var (
		headers = []string{"Metric", "Value"}
		rows    = [][]string{
			{"Total Tests", f.colors.Bold(fmt.Sprintf("%d", summary.TotalTests))},
			{"Passed", passedValue},
			{"Failed", failedValue},
			{"Total Duration", format.Duration(summary.TotalDuration)},
			{"Cache Hit Rate", cacheValue},
			{"Total Data Loaded", format.Bytes(summary.TotalDataSize)},
		}
	)

	return "\n" + f.colors.Header("▸ Summary") + "\n\n" + f.renderer.RenderToString(headers, rows)
}
