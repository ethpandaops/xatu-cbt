package table

import (
	"fmt"

	"github.com/fatih/color"
)

// ColorHelper provides utilities for coloring test output
type ColorHelper struct {
	enabled bool
}

// NewColorHelper creates a new color helper
// Colors are enabled only when outputting to a terminal
func NewColorHelper() *ColorHelper {
	return &ColorHelper{
		enabled: !color.NoColor,
	}
}

// Success returns green colored text
func (c *ColorHelper) Success(text string) string {
	if !c.enabled {
		return text
	}
	return color.GreenString(text)
}

// Failure returns red colored text
func (c *ColorHelper) Failure(text string) string {
	if !c.enabled {
		return text
	}
	return color.RedString(text)
}

// Warning returns yellow colored text
func (c *ColorHelper) Warning(text string) string {
	if !c.enabled {
		return text
	}
	return color.YellowString(text)
}

// Info returns cyan colored text
func (c *ColorHelper) Info(text string) string {
	if !c.enabled {
		return text
	}
	return color.CyanString(text)
}

// Muted returns gray colored text
func (c *ColorHelper) Muted(text string) string {
	if !c.enabled {
		return text
	}
	return color.New(color.FgHiBlack).Sprint(text)
}

// Bold returns bold text
func (c *ColorHelper) Bold(text string) string {
	if !c.enabled {
		return text
	}
	return color.New(color.Bold).Sprint(text)
}

// Header returns bold cyan text for section headers
func (c *ColorHelper) Header(text string) string {
	if !c.enabled {
		return text
	}
	return color.New(color.FgCyan, color.Bold).Sprint(text)
}

// FormatStatus returns appropriately colored status text
func (c *ColorHelper) FormatStatus(passed bool) string {
	if passed {
		return c.Success("✓ PASS")
	}
	return c.Failure("✗ FAIL")
}

// FormatAssertions returns colored assertion text based on pass/fail
func (c *ColorHelper) FormatAssertions(passed, total int) string {
	text := fmt.Sprintf("%d/%d", passed, total)
	if passed == total {
		return c.Success(text)
	}
	if passed == 0 {
		return c.Failure(text)
	}
	return c.Warning(text)
}

// FormatPercentage returns colored percentage based on value
func (c *ColorHelper) FormatPercentage(value float64) string {
	text := fmt.Sprintf("%.1f%%", value)
	if value == 100.0 {
		return c.Success(text)
	}
	if value >= 90.0 {
		return c.Warning(text)
	}
	return c.Failure(text)
}
