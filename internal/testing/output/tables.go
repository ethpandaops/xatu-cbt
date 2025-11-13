package output

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
)

// ParquetLoadSource indicates where parquet data was loaded from.
type ParquetLoadSource string

const (
	// SourceCache defines if parquet was loaded via cache.
	SourceCache ParquetLoadSource = "cache"
	// SourceS3 defines if parquet was loaded via s3.
	SourceS3 ParquetLoadSource = "s3"
)

// ParquetLoadMetric captures metrics about loading a parquet file.
type ParquetLoadMetric struct {
	Table     string
	Source    ParquetLoadSource
	SizeBytes int64
	Duration  time.Duration
	Timestamp time.Time
}

// FailedAssertionDetail captures details about a single failed assertion.
type FailedAssertionDetail struct {
	Name     string
	Expected map[string]interface{}
	Actual   map[string]interface{}
	Error    string
}

// TestResultMetric captures metrics about a test execution.
type TestResultMetric struct {
	Model            string
	Passed           bool
	Duration         time.Duration
	AssertionsTotal  int
	AssertionsPassed int
	AssertionsFailed int
	ErrorMessage     string
	FailedAssertions []FailedAssertionDetail
	Timestamp        time.Time
}

// SummaryMetric provides aggregate statistics across all operations.
type SummaryMetric struct {
	TotalDuration time.Duration
	TotalTests    int
	PassedTests   int
	FailedTests   int
	CacheHits     int
	CacheMisses   int
	CacheHitRate  float64
	TotalDataSize int64
}

// TableRenderer provides table rendering utilities using tablewriter.
type TableRenderer struct {
	log logrus.FieldLogger
}

// NewTableRenderer creates a new table renderer.
func NewTableRenderer(log logrus.FieldLogger) *TableRenderer {
	return &TableRenderer{
		log: log.WithField("component", "table_renderer"),
	}
}

// RenderToString renders a table to a string with the given headers and rows.
func (r *TableRenderer) RenderToString(headers []string, rows [][]string) string {
	buf := &bytes.Buffer{}
	r.RenderToWriter(buf, headers, rows)
	return buf.String()
}

// RenderToWriter renders a table to the given writer with headers and rows.
func (r *TableRenderer) RenderToWriter(w io.Writer, headers []string, rows [][]string) {
	table := tablewriter.NewWriter(w)
	table.SetHeader(headers)

	// Apply consistent styling
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("│")
	table.SetRowSeparator("─")
	table.SetHeaderLine(true)
	table.SetBorder(true)
	table.SetTablePadding(" ")
	table.SetNoWhiteSpace(false)

	table.AppendBulk(rows)
	table.Render()
}

// Color helper functions - simplified from the original ColorHelper
var (
	colorEnabled = !color.NoColor
)

func colorSuccess(text string) string {
	if !colorEnabled {
		return text
	}
	return color.GreenString(text)
}

func colorFailure(text string) string {
	if !colorEnabled {
		return text
	}
	return color.RedString(text)
}

func colorWarning(text string) string {
	if !colorEnabled {
		return text
	}
	return color.YellowString(text)
}

func colorInfo(text string) string {
	if !colorEnabled {
		return text
	}
	return color.CyanString(text)
}

func colorMuted(text string) string {
	if !colorEnabled {
		return text
	}
	return color.New(color.FgHiBlack).Sprint(text)
}

func colorBold(text string) string {
	if !colorEnabled {
		return text
	}
	return color.New(color.Bold).Sprint(text)
}

func colorHeader(text string) string {
	if !colorEnabled {
		return text
	}
	return color.New(color.FgCyan, color.Bold).Sprint(text)
}

func formatStatus(passed bool) string {
	if passed {
		return colorSuccess("✓ PASS")
	}
	return colorFailure("✗ FAIL")
}

func formatAssertions(passed, total int) string {
	text := fmt.Sprintf("%d/%d", passed, total)
	if passed == total {
		return colorSuccess(text)
	}
	if passed == 0 {
		return colorFailure(text)
	}
	return colorWarning(text)
}

func formatPercentage(value float64) string {
	text := fmt.Sprintf("%.1f%%", value)
	if value == 100.0 {
		return colorSuccess(text)
	}
	if value >= 90.0 {
		return colorWarning(text)
	}
	return colorFailure(text)
}

// FormatParquetMetrics formats parquet loading metrics as a table.
func FormatParquetMetrics(renderer *TableRenderer, parquetMetrics []ParquetLoadMetric) string {
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

	return "\n▸ Parquet Files Loaded\n\n" + renderer.RenderToString(headers, rows)
}

// FormatTestResults formats test result metrics as a table with failure details.
func FormatTestResults(renderer *TableRenderer, testMetrics []TestResultMetric) string {
	if len(testMetrics) == 0 {
		return "No tests executed"
	}

	headers := []string{"Model", "Status", "Assertions", "Details"}
	rows := make([][]string, 0, len(testMetrics))
	failedTests := make([]TestResultMetric, 0)

	for _, metric := range testMetrics {
		status := formatStatus(metric.Passed)
		details := formatTestDetails(&metric, &failedTests)
		assertionInfo := formatAssertions(metric.AssertionsPassed, metric.AssertionsTotal)

		rows = append(rows, []string{
			metric.Model,
			status,
			assertionInfo,
			details,
		})
	}

	output := "\n" + colorHeader("▸ Test Results") + "\n\n" + renderer.RenderToString(headers, rows)

	if len(failedTests) > 0 {
		output += formatFailureDetails(failedTests)
	}

	return output
}

func formatTestDetails(metric *TestResultMetric, failedTests *[]TestResultMetric) string {
	if metric.Passed {
		return ""
	}

	*failedTests = append(*failedTests, *metric)

	var details string

	if metric.AssertionsFailed > 0 {
		details = colorFailure(fmt.Sprintf("%d/%d failed",
			metric.AssertionsFailed,
			metric.AssertionsTotal))
	}

	if metric.ErrorMessage != "" {
		errMsg := metric.ErrorMessage
		if len(errMsg) > 50 {
			errMsg = errMsg[:47] + "..."
		}
		if details != "" {
			details += " - "
		}
		details += colorMuted(errMsg)
	}

	return details
}

func formatFailureDetails(failedTests []TestResultMetric) string {
	var builder strings.Builder

	builder.WriteString("\n\n" + colorHeader("▸ Failed Test Details") + "\n\n")

	for i, test := range failedTests {
		if i > 0 {
			builder.WriteString("\n")
		}

		builder.WriteString(fmt.Sprintf("%s (%s)\n", test.Model, formatDuration(test.Duration)))
		formatTestFailureDetails(&builder, &test)
	}

	return builder.String()
}

func formatTestFailureDetails(builder *strings.Builder, test *TestResultMetric) {
	if len(test.FailedAssertions) == 0 {
		formatTestErrorMessage(builder, test.ErrorMessage)
		return
	}

	for _, assertion := range test.FailedAssertions {
		formatAssertionFailure(builder, &assertion)
	}
}

func formatTestErrorMessage(builder *strings.Builder, errorMessage string) {
	if errorMessage != "" {
		fmt.Fprintf(builder, "  %s: %s\n",
			colorFailure("Error"),
			errorMessage)
		return
	}

	fmt.Fprintf(builder, "  %s: Test failed (no details available)\n",
		colorFailure("Error"))
}

func formatAssertionFailure(builder *strings.Builder, assertion *FailedAssertionDetail) {
	fmt.Fprintf(builder, "  %s %s: %s\n",
		colorFailure("✗"),
		colorBold("Assertion"),
		assertion.Name)

	if len(assertion.Expected) > 0 {
		fmt.Fprintf(builder, "    %s: %v\n",
			colorInfo("Expected"),
			assertion.Expected)
	}

	if len(assertion.Actual) > 0 {
		fmt.Fprintf(builder, "    %s: %v\n",
			colorWarning("Actual"),
			assertion.Actual)
	}

	if assertion.Error != "" {
		fmt.Fprintf(builder, "    %s: %s\n",
			colorFailure("Error"),
			assertion.Error)
	}
}

// FormatSummary formats summary statistics as a table.
func FormatSummary(renderer *TableRenderer, summary SummaryMetric) string {
	var passRate float64
	if summary.TotalTests > 0 {
		passRate = float64(summary.PassedTests) / float64(summary.TotalTests) * 100.0
	}

	// Format values with colors
	passedValue := fmt.Sprintf("%d (%s)", summary.PassedTests, formatPercentage(passRate))
	if summary.PassedTests == summary.TotalTests {
		passedValue = colorSuccess(fmt.Sprintf("%d (%.1f%%)", summary.PassedTests, passRate))
	}

	failedValue := fmt.Sprintf("%d (%.1f%%)", summary.FailedTests, 100.0-passRate)
	if summary.FailedTests > 0 {
		failedValue = colorFailure(fmt.Sprintf("%d (%.1f%%)", summary.FailedTests, 100.0-passRate))
	} else {
		failedValue = colorSuccess(failedValue)
	}

	cacheValue := fmt.Sprintf("%.1f%% (%d/%d)",
		summary.CacheHitRate,
		summary.CacheHits,
		summary.CacheHits+summary.CacheMisses)

	switch {
	case summary.CacheHitRate == 100.0:
		cacheValue = colorSuccess(cacheValue)
	case summary.CacheHitRate >= 50.0:
		cacheValue = colorWarning(cacheValue)
	default:
		cacheValue = colorMuted(cacheValue)
	}

	headers := []string{"Metric", "Value"}
	rows := [][]string{
		{"Total Tests", colorBold(fmt.Sprintf("%d", summary.TotalTests))},
		{"Passed", passedValue},
		{"Failed", failedValue},
		{"Total Duration", formatDuration(summary.TotalDuration)},
		{"Cache Hit Rate", cacheValue},
		{"Total Data Loaded", formatBytes(summary.TotalDataSize)},
	}

	return "\n" + colorHeader("▸ Summary") + "\n\n" + renderer.RenderToString(headers, rows)
}
