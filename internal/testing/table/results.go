package table

import (
	"context"
	"fmt"
	"strings"

	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/sirupsen/logrus"
)

// ResultsFormatter formats test results as a table
type ResultsFormatter interface {
	Start(ctx context.Context) error
	Stop() error
	Format(metrics []metrics.TestResultMetric) string
}

type resultsFormatter struct {
	log      logrus.FieldLogger
	renderer Renderer
}

// NewResultsFormatter creates a new results table formatter
func NewResultsFormatter(log logrus.FieldLogger, renderer Renderer) ResultsFormatter {
	return &resultsFormatter{
		log:      log.WithField("component", "table.results_formatter"),
		renderer: renderer,
	}
}

func (f *resultsFormatter) Start(_ context.Context) error {
	f.log.Info("results formatter started")
	return nil
}

func (f *resultsFormatter) Stop() error {
	f.log.Info("results formatter stopped")
	return nil
}

func (f *resultsFormatter) Format(testMetrics []metrics.TestResultMetric) string {
	if len(testMetrics) == 0 {
		return "No tests executed"
	}

	headers := []string{"Model", "Status", "Assertions", "Duration", "Details"}
	rows := make([][]string, 0, len(testMetrics))
	failedTests := make([]metrics.TestResultMetric, 0)

	for _, metric := range testMetrics {
		status := "✓ PASS"
		details := ""

		if !metric.Passed {
			status = "✗ FAIL"
			failedTests = append(failedTests, metric)

			if metric.AssertionsFailed > 0 {
				details = fmt.Sprintf("%d/%d failed",
					metric.AssertionsFailed,
					metric.AssertionsTotal)
			}
			if metric.ErrorMessage != "" {
				// Truncate long error messages
				errMsg := metric.ErrorMessage
				if len(errMsg) > 50 {
					errMsg = errMsg[:47] + "..."
				}
				if details != "" {
					details += " - "
				}
				details += errMsg
			}
		}

		assertionInfo := fmt.Sprintf("%d/%d",
			metric.AssertionsPassed,
			metric.AssertionsTotal)

		rows = append(rows, []string{
			metric.Model,
			status,
			assertionInfo,
			formatDuration(metric.Duration),
			details,
		})
	}

	output := "\n▸ Test Results\n\n" + f.renderer.RenderToString(headers, rows)

	// Add detailed failure section if there are any failures
	if len(failedTests) > 0 {
		output += f.formatFailureDetails(failedTests)
	}

	return output
}

// formatFailureDetails creates a detailed section showing all failed assertions
func (f *resultsFormatter) formatFailureDetails(failedTests []metrics.TestResultMetric) string {
	var builder strings.Builder

	builder.WriteString("\n\n▸ Failed Test Details\n\n")

	for i, test := range failedTests {
		if i > 0 {
			builder.WriteString("\n")
		}

		builder.WriteString(fmt.Sprintf("%s (%s)\n", test.Model, formatDuration(test.Duration)))

		if len(test.FailedAssertions) == 0 {
			// No specific assertion details, show general error
			if test.ErrorMessage != "" {
				builder.WriteString(fmt.Sprintf("  Error: %s\n", test.ErrorMessage))
			} else {
				builder.WriteString("  Error: Test failed (no details available)\n")
			}
		} else {
			// Show detailed assertion failures
			for _, assertion := range test.FailedAssertions {
				builder.WriteString(fmt.Sprintf("  ✗ Assertion: %s\n", assertion.Name))

				if len(assertion.Expected) > 0 {
					builder.WriteString(fmt.Sprintf("    Expected: %v\n", formatMap(assertion.Expected)))
				}

				if len(assertion.Actual) > 0 {
					builder.WriteString(fmt.Sprintf("    Actual:   %v\n", formatMap(assertion.Actual)))
				}

				if assertion.Error != "" {
					builder.WriteString(fmt.Sprintf("    Error:    %s\n", assertion.Error))
				}
			}
		}
	}

	return builder.String()
}

// formatMap formats a map for display in error messages
func formatMap(m map[string]interface{}) string {
	if len(m) == 0 {
		return "{}"
	}

	// Use a simple format for small maps
	return fmt.Sprintf("%v", m)
}

// Compile-time interface compliance check
var _ ResultsFormatter = (*resultsFormatter)(nil)
