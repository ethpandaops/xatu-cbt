// Package output provides formatted console output for test results.
package output

import (
	"fmt"
	"io"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/testing/format"
	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/ethpandaops/xatu-cbt/internal/testing/table"
	"github.com/fatih/color"
)

// Formatter provides clean, human-friendly output.
type Formatter interface {
	PrintPhase(phase string)
	PrintProgress(message string, duration time.Duration)
	PrintSuccess(message string)
	PrintError(message string, err error)
	PrintParquetSummary()
	PrintTestResults()
	PrintSummary()
}

type formatter struct {
	writer  io.Writer
	verbose bool

	// Table formatting components
	metrics          metrics.Collector
	tableRenderer    *table.Renderer
	parquetFormatter *table.ParquetFormatter
	resultsFormatter *table.ResultsFormatter
	summaryFormatter *table.SummaryFormatter

	// Colors
	green  *color.Color
	red    *color.Color
	yellow *color.Color
	blue   *color.Color
	gray   *color.Color
}

// NewFormatter creates a new output formatter.
func NewFormatter(
	writer io.Writer,
	verbose bool,
	metricsCollector metrics.Collector,
	tableRenderer *table.Renderer,
	parquetFormatter *table.ParquetFormatter,
	resultsFormatter *table.ResultsFormatter,
	summaryFormatter *table.SummaryFormatter,
) Formatter {
	return &formatter{
		writer:           writer,
		verbose:          verbose,
		metrics:          metricsCollector,
		tableRenderer:    tableRenderer,
		parquetFormatter: parquetFormatter,
		resultsFormatter: resultsFormatter,
		summaryFormatter: summaryFormatter,
		green:            color.New(color.FgGreen),
		red:              color.New(color.FgRed),
		yellow:           color.New(color.FgYellow),
		blue:             color.New(color.FgBlue),
		gray:             color.New(color.FgHiBlack),
	}
}

// PrintPhase prints phase separator.
func (f *formatter) PrintPhase(phase string) {
	_, _ = f.blue.Fprintf(f.writer, "\n▸ %s\n", phase) // Ignore write errors to stdout
}

// PrintProgress prints progress with checkmark and timing.
func (f *formatter) PrintProgress(message string, duration time.Duration) {
	if duration > 0 {
		_, _ = f.gray.Fprintf(f.writer, "%s (%s)\n", message, format.Duration(duration)) // Ignore write errors to stdout
	} else {
		_, _ = fmt.Fprintf(f.writer, "%s\n", message) // Ignore write errors to stdout
	}
}

// PrintSuccess prints green checkmark + message.
func (f *formatter) PrintSuccess(message string) {
	_, _ = f.green.Fprintf(f.writer, "%s\n", message) // Ignore write errors to stdout
}

// PrintError prints red X + message + error details.
func (f *formatter) PrintError(message string, err error) {
	_, _ = f.red.Fprintf(f.writer, "%s", message) // Ignore write errors to stdout
	if err != nil {
		_, _ = f.red.Fprintf(f.writer, ": %v", err) // Ignore write errors to stdout
	}
	_, _ = fmt.Fprintf(f.writer, "\n") // Ignore write errors to stdout
}

// PrintParquetSummary prints a table summary of parquet files loaded.
func (f *formatter) PrintParquetSummary() {
	parquetMetrics := f.metrics.GetParquetMetrics()
	output := f.parquetFormatter.Format(parquetMetrics)
	_, _ = fmt.Fprintln(f.writer, output) // Ignore write errors to stdout
}

// PrintTestResults prints a table of test results.
func (f *formatter) PrintTestResults() {
	testMetrics := f.metrics.GetTestMetrics()
	output := f.resultsFormatter.Format(testMetrics)
	_, _ = fmt.Fprintln(f.writer, output) // Ignore write errors to stdout
}

// PrintSummary prints a summary table with aggregate statistics.
func (f *formatter) PrintSummary() {
	summary := f.metrics.GetSummary()
	output := f.summaryFormatter.Format(summary)
	_, _ = fmt.Fprintln(f.writer, output) // Ignore write errors to stdout
}
