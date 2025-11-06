package output

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/fatih/color"
)

// Formatter provides clean, human-friendly output
type Formatter interface {
	PrintHeader(network, spec string, modelCount int)
	PrintPhase(phase string)
	PrintProgress(message string, duration time.Duration)
	PrintSuccess(message string)
	PrintError(message string, err error)
}

type formatter struct {
	writer  io.Writer
	verbose bool

	// Colors
	green  *color.Color
	red    *color.Color
	yellow *color.Color
	blue   *color.Color
	gray   *color.Color
}

// NewFormatter creates a new output formatter
func NewFormatter(writer io.Writer, verbose bool) Formatter {
	return &formatter{
		writer:  writer,
		verbose: verbose,
		green:   color.New(color.FgGreen),
		red:     color.New(color.FgRed),
		yellow:  color.New(color.FgYellow),
		blue:    color.New(color.FgBlue),
		gray:    color.New(color.FgHiBlack),
	}
}

// PrintHeader prints test header with network/spec info
func (f *formatter) PrintHeader(network, spec string, modelCount int) {
	width := 60
	fmt.Fprintf(f.writer, "\n%s\n", strings.Repeat("─", width))
	f.blue.Fprintf(f.writer, "│ xatu-cbt test runner%s│\n", strings.Repeat(" ", width-23))
	fmt.Fprintf(f.writer, "%s\n", strings.Repeat("─", width))
	fmt.Fprintf(f.writer, "│ Spec: %-10s  Network: %-10s%s│\n",
		spec, network, strings.Repeat(" ", width-38))

	if modelCount > 0 {
		fmt.Fprintf(f.writer, "│ Models: %-5d%s│\n",
			modelCount, strings.Repeat(" ", width-17))
	}

	fmt.Fprintf(f.writer, "%s\n\n", strings.Repeat("─", width))
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
