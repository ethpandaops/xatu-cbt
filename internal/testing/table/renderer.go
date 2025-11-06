package table

import (
	"bytes"
	"io"

	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
)

// Renderer provides table rendering utilities.
type Renderer struct {
	log logrus.FieldLogger
}

// NewRenderer creates a new table renderer.
func NewRenderer(log logrus.FieldLogger) *Renderer {
	return &Renderer{
		log: log.WithField("component", "table.renderer"),
	}
}

// RenderOption configures table rendering.
type RenderOption func(*tablewriter.Table)

// WithAlignment sets column alignment (use tablewriter constants)
func WithAlignment(alignment int) RenderOption {
	return func(t *tablewriter.Table) {
		t.SetAlignment(alignment)
	}
}

// WithBorder controls border visibility.
func WithBorder(show bool) RenderOption {
	return func(t *tablewriter.Table) {
		t.SetBorder(show)
	}
}

// WithAutoFormatHeaders enables/disables auto header formatting.
func WithAutoFormatHeaders(enable bool) RenderOption {
	return func(t *tablewriter.Table) {
		t.SetAutoFormatHeaders(enable)
	}
}

// WithRowSeparator controls row separator lines.
func WithRowSeparator(show bool) RenderOption {
	return func(t *tablewriter.Table) {
		t.SetRowSeparator("")
		if show {
			t.SetRowSeparator("-")
		}
	}
}

// RenderToString renders a table to a string with the given headers and rows.
func (r *Renderer) RenderToString(headers []string, rows [][]string, opts ...RenderOption) string {
	buf := &bytes.Buffer{}
	r.RenderToWriter(buf, headers, rows, opts...)
	return buf.String()
}

// RenderToWriter renders a table to the given writer with headers and rows.
func (r *Renderer) RenderToWriter(w io.Writer, headers []string, rows [][]string, opts ...RenderOption) {
	table := tablewriter.NewWriter(w)
	table.SetHeader(headers)

	// Apply default styling
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

	// Apply custom options
	for _, opt := range opts {
		opt(table)
	}

	// Add rows
	table.AppendBulk(rows)

	// Render
	table.Render()
}
