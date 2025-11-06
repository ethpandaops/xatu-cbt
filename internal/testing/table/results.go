package table

import (
	"fmt"
	"strings"

	"github.com/ethpandaops/xatu-cbt/internal/testing/format"
	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/sirupsen/logrus"
)

// ResultsFormatter formats test results as a table.
type ResultsFormatter struct {
	log      logrus.FieldLogger
	renderer *Renderer
	colors   *ColorHelper
}

// NewResultsFormatter creates a new results table formatter.
func NewResultsFormatter(log logrus.FieldLogger, renderer *Renderer) *ResultsFormatter {
	return &ResultsFormatter{
		log:      log.WithField("component", "table.results_formatter"),
		renderer: renderer,
		colors:   NewColorHelper(),
	}
}

// Format converts test result metrics into a formatted table string with failure details.
func (f *ResultsFormatter) Format(testMetrics []metrics.TestResultMetric) string {
	if len(testMetrics) == 0 {
		return "No tests executed"
	}

	var (
		headers     = []string{"Model", "Status", "Assertions", "Duration", "Details"}
		rows        = make([][]string, 0, len(testMetrics))
		failedTests = make([]metrics.TestResultMetric, 0)
	)

	for _, metric := range testMetrics {
		var (
			status  = f.colors.FormatStatus(metric.Passed)
			details string
		)

		if !metric.Passed { //nolint:nestif // Test result formatting - refactoring risky
			failedTests = append(failedTests, metric)

			if metric.AssertionsFailed > 0 {
				details = f.colors.Failure(fmt.Sprintf("%d/%d failed",
					metric.AssertionsFailed,
					metric.AssertionsTotal))
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

				details += f.colors.Muted(errMsg)
			}
		}

		assertionInfo := f.colors.FormatAssertions(
			metric.AssertionsPassed,
			metric.AssertionsTotal,
		)

		rows = append(rows, []string{
			metric.Model,
			status,
			assertionInfo,
			format.Duration(metric.Duration),
			details,
		})
	}

	output := "\n" + f.colors.Header("▸ Test Results") + "\n\n" + f.renderer.RenderToString(headers, rows)

	// Add detailed failure section if there are any failures
	if len(failedTests) > 0 {
		output += f.formatFailureDetails(failedTests)
	}

	return output
}

// formatFailureDetails creates a detailed section showing all failed assertions
func (f *ResultsFormatter) formatFailureDetails(failedTests []metrics.TestResultMetric) string {
	var builder strings.Builder

	builder.WriteString("\n\n" + f.colors.Header("▸ Failed Test Details") + "\n\n")

	for i, test := range failedTests {
		if i > 0 {
			builder.WriteString("\n")
		}

		builder.WriteString(fmt.Sprintf("%s (%s)\n", test.Model, format.Duration(test.Duration)))

		if len(test.FailedAssertions) == 0 { //nolint:nestif // Error formatting logic - refactoring risky
			// No specific assertion details, show general error
			if test.ErrorMessage != "" {
				builder.WriteString(
					fmt.Sprintf("  %s: %s\n",
						f.colors.Failure("Error"),
						test.ErrorMessage,
					),
				)
			} else {
				builder.WriteString(
					fmt.Sprintf(
						"  %s: Test failed (no details available)\n",
						f.colors.Failure("Error"),
					),
				)
			}
		} else {
			// Show detailed assertion failures
			for _, assertion := range test.FailedAssertions {
				builder.WriteString(
					fmt.Sprintf("  %s %s: %s\n",
						f.colors.Failure("✗"),
						f.colors.Bold("Assertion"),
						assertion.Name,
					),
				)

				if len(assertion.Expected) > 0 {
					builder.WriteString(
						fmt.Sprintf("    %s: %v\n",
							f.colors.Info("Expected"),
							formatMap(assertion.Expected),
						),
					)
				}

				if len(assertion.Actual) > 0 {
					builder.WriteString(
						fmt.Sprintf("    %s: %v\n",
							f.colors.Warning("Actual"),
							formatMap(assertion.Actual),
						),
					)
				}

				if assertion.Error != "" {
					builder.WriteString(
						fmt.Sprintf("    %s: %s\n",
							f.colors.Failure("Error"),
							assertion.Error,
						),
					)
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
