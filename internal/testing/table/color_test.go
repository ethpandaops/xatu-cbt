package table

import (
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
)

func TestColorHelper_FormatStatus(t *testing.T) {
	// Disable colors for consistent testing
	color.NoColor = true
	defer func() { color.NoColor = false }()

	helper := NewColorHelper()

	t.Run("passed status", func(t *testing.T) {
		result := helper.FormatStatus(true)
		assert.Equal(t, "✓ PASS", result)
	})

	t.Run("failed status", func(t *testing.T) {
		result := helper.FormatStatus(false)
		assert.Equal(t, "✗ FAIL", result)
	})
}

func TestColorHelper_FormatAssertions(t *testing.T) {
	// Disable colors for consistent testing
	color.NoColor = true
	defer func() { color.NoColor = false }()

	helper := NewColorHelper()

	tests := []struct {
		name     string
		passed   int
		total    int
		expected string
	}{
		{
			name:     "all passed",
			passed:   5,
			total:    5,
			expected: "5/5",
		},
		{
			name:     "partial pass",
			passed:   3,
			total:    5,
			expected: "3/5",
		},
		{
			name:     "all failed",
			passed:   0,
			total:    5,
			expected: "0/5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := helper.FormatAssertions(tt.passed, tt.total)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestColorHelper_FormatPercentage(t *testing.T) {
	// Disable colors for consistent testing
	color.NoColor = true
	defer func() { color.NoColor = false }()

	helper := NewColorHelper()

	tests := []struct {
		name     string
		value    float64
		expected string
	}{
		{
			name:     "100%",
			value:    100.0,
			expected: "100.0%",
		},
		{
			name:     "90%",
			value:    90.0,
			expected: "90.0%",
		},
		{
			name:     "50%",
			value:    50.0,
			expected: "50.0%",
		},
		{
			name:     "0%",
			value:    0.0,
			expected: "0.0%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := helper.FormatPercentage(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestColorHelper_ColorsDisabledWhenNoColor(t *testing.T) {
	// Enable NoColor flag
	color.NoColor = true
	defer func() { color.NoColor = false }()

	helper := NewColorHelper()
	assert.False(t, helper.enabled)

	// Should return plain text
	assert.Equal(t, "test", helper.Success("test"))
	assert.Equal(t, "test", helper.Failure("test"))
	assert.Equal(t, "test", helper.Warning("test"))
}
