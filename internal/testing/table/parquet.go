// Package table provides table formatting for test results and metrics.
package table

import (
	"github.com/ethpandaops/xatu-cbt/internal/testing/format"
	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/sirupsen/logrus"
)

// ParquetFormatter formats parquet loading metrics as a table
type ParquetFormatter struct {
	log      logrus.FieldLogger
	renderer *Renderer
}

// NewParquetFormatter creates a new parquet table formatter
func NewParquetFormatter(log logrus.FieldLogger, renderer *Renderer) *ParquetFormatter {
	return &ParquetFormatter{
		log:      log.WithField("component", "table.parquet_formatter"),
		renderer: renderer,
	}
}

// Format converts parquet loading metrics into a formatted table string
func (f *ParquetFormatter) Format(parquetMetrics []metrics.ParquetLoadMetric) string {
	if len(parquetMetrics) == 0 {
		return "No parquet files loaded"
	}

	headers := []string{"Table", "Source", "Size", "Duration"}
	rows := make([][]string, 0, len(parquetMetrics))

	for _, metric := range parquetMetrics {
		rows = append(rows, []string{
			metric.Table,
			string(metric.Source),
			format.Bytes(metric.SizeBytes),
			format.Duration(metric.Duration),
		})
	}

	return "\n▸ Parquet Files Loaded\n\n" + f.renderer.RenderToString(headers, rows)
}
