// Package output provides formatted console output for test results.
package output

import (
	"fmt"
	"io"
	"time"

	"github.com/sirupsen/logrus"
)

// MetricsProvider defines the interface for accessing test metrics.
// This allows the output package to remain independent of the testing package.
type MetricsProvider interface {
	GetParquetMetrics() []ParquetLoadMetric
	GetTestMetrics() []TestResultMetric
	GetSummary() SummaryMetric
}

// Formatter provides clean, human-friendly output for test results.
// This is the concrete implementation without an interface abstraction.
type Formatter struct {
	writer        io.Writer
	verbose       bool
	metrics       MetricsProvider
	tableRenderer *TableRenderer
}

// NewFormatter creates a new output formatter.
func NewFormatter(
	log logrus.FieldLogger,
	writer io.Writer,
	verbose bool,
	metricsCollector MetricsProvider,
) *Formatter {
	return &Formatter{
		writer:        writer,
		verbose:       verbose,
		metrics:       metricsCollector,
		tableRenderer: NewTableRenderer(log),
	}
}

// PrintParquetSummary prints a table summary of parquet files loaded.
func (f *Formatter) PrintParquetSummary() {
	parquetMetrics := f.metrics.GetParquetMetrics()
	output := FormatParquetMetrics(f.tableRenderer, parquetMetrics)
	_, _ = fmt.Fprintln(f.writer, output) // Ignore write errors to stdout
}

// PrintTestResults prints a table of test results.
func (f *Formatter) PrintTestResults() {
	testMetrics := f.metrics.GetTestMetrics()
	output := FormatTestResults(f.tableRenderer, testMetrics)
	_, _ = fmt.Fprintln(f.writer, output) // Ignore write errors to stdout
}

// PrintSummary prints a summary table with aggregate statistics.
func (f *Formatter) PrintSummary() {
	summary := f.metrics.GetSummary()
	output := FormatSummary(f.tableRenderer, summary)
	_, _ = fmt.Fprintln(f.writer, output) // Ignore write errors to stdout
}

// formatDuration formats a duration for human-readable output.
// Handles microseconds, milliseconds, seconds, and minutes.
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

// formatBytes converts bytes to human-readable format (KiB, MiB, GiB, etc.)
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
