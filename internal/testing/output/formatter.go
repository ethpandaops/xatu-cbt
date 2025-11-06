package output

import (
	"fmt"
	"io"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/ethpandaops/xatu-cbt/internal/testing/table"
	"github.com/fatih/color"
)

// Formatter provides clean, human-friendly output
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
	tableRenderer    table.Renderer
	parquetFormatter table.ParquetFormatter
	resultsFormatter table.ResultsFormatter
	summaryFormatter table.SummaryFormatter

	// Colors
	green  *color.Color
	red    *color.Color
	yellow *color.Color
	blue   *color.Color
	gray   *color.Color
}

// NewFormatter creates a new output formatter
func NewFormatter(
	writer io.Writer,
	verbose bool,
	metricsCollector metrics.Collector,
	tableRenderer table.Renderer,
	parquetFormatter table.ParquetFormatter,
	resultsFormatter table.ResultsFormatter,
	summaryFormatter table.SummaryFormatter,
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

// PrintPhase prints phase separator
func (f *formatter) PrintPhase(phase string) {
	f.blue.Fprintf(f.writer, "\n▸ %s\n", phase)
}

// PrintProgress prints progress with checkmark and timing
func (f *formatter) PrintProgress(message string, duration time.Duration) {
	if duration > 0 {
		f.gray.Fprintf(f.writer, "%s (%s)\n", message, formatDuration(duration))
	} else {
		fmt.Fprintf(f.writer, "%s\n", message)
	}
}

// PrintSuccess prints green checkmark + message
func (f *formatter) PrintSuccess(message string) {
	f.green.Fprintf(f.writer, "%s\n", message)
}

// PrintError prints red X + message + error details
func (f *formatter) PrintError(message string, err error) {
	f.red.Fprintf(f.writer, "%s", message)
	if err != nil {
		f.red.Fprintf(f.writer, ": %v", err)
	}
	fmt.Fprintf(f.writer, "\n")
}

// formatDuration formats duration in human-readable format
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
}

// PrintParquetSummary prints a table summary of parquet files loaded
func (f *formatter) PrintParquetSummary() {
	parquetMetrics := f.metrics.GetParquetMetrics()
	output := f.parquetFormatter.Format(parquetMetrics)
	fmt.Fprintln(f.writer, output)
}

// PrintTestResults prints a table of test results
func (f *formatter) PrintTestResults() {
	testMetrics := f.metrics.GetTestMetrics()
	output := f.resultsFormatter.Format(testMetrics)
	fmt.Fprintln(f.writer, output)
}

// PrintSummary prints a summary table with aggregate statistics
func (f *formatter) PrintSummary() {
	summary := f.metrics.GetSummary()
	output := f.summaryFormatter.Format(summary)
	fmt.Fprintln(f.writer, output)
}
