// Package table provides table formatting for test results and metrics.
package table

import (
	"context"
	"fmt"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/sirupsen/logrus"
)

// ParquetFormatter formats parquet loading metrics as a table
type ParquetFormatter interface {
	Start(ctx context.Context) error
	Stop() error
	Format(metrics []metrics.ParquetLoadMetric) string
}

type parquetFormatter struct {
	log      logrus.FieldLogger
	renderer Renderer
}

// NewParquetFormatter creates a new parquet table formatter
func NewParquetFormatter(log logrus.FieldLogger, renderer Renderer) ParquetFormatter {
	return &parquetFormatter{
		log:      log.WithField("component", "table.parquet_formatter"),
		renderer: renderer,
	}
}

func (f *parquetFormatter) Start(_ context.Context) error {
	f.log.Info("parquet formatter started")

	return nil
}

func (f *parquetFormatter) Stop() error {
	f.log.Info("parquet formatter stopped")

	return nil
}

func (f *parquetFormatter) Format(parquetMetrics []metrics.ParquetLoadMetric) string {
	if len(parquetMetrics) == 0 {
		return "No parquet files loaded"
	}

	headers := []string{"Table", "Source", "Size", "Duration"}
	rows := make([][]string, 0, len(parquetMetrics))

	for _, metric := range parquetMetrics {
		rows = append(rows, []string{
			metric.Table,
			string(metric.Source),
			formatBytes(metric.SizeBytes),
			formatDuration(metric.Duration),
		})
	}

	return "\n▸ Parquet Files Loaded\n\n" + f.renderer.RenderToString(headers, rows)
}

// formatBytes converts bytes to human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatDuration formats duration in human-readable format
func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%.0fms", float64(d.Milliseconds()))
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
}

// Compile-time interface compliance check
var _ ParquetFormatter = (*parquetFormatter)(nil)
