package table

import (
	"bytes"
	"context"
	"io"

	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
)

// Renderer provides table rendering utilities
type Renderer interface {
	Start(ctx context.Context) error
	Stop() error
	RenderToString(headers []string, rows [][]string, opts ...RenderOption) string
	RenderToWriter(w io.Writer, headers []string, rows [][]string, opts ...RenderOption)
}

// renderer implements Renderer interface
type renderer struct {
	log logrus.FieldLogger
}

// NewRenderer creates a new table renderer
func NewRenderer(log logrus.FieldLogger) Renderer {
	return &renderer{
		log: log.WithField("component", "table.renderer"),
	}
}

func (r *renderer) Start(ctx context.Context) error {
	r.log.Info("table renderer started")
	return nil
}

func (r *renderer) Stop() error {
	r.log.Info("table renderer stopped")
	return nil
}

// RenderOption configures table rendering
type RenderOption func(*tablewriter.Table)

// WithAlignment sets column alignment (use tablewriter constants)
func WithAlignment(alignment int) RenderOption {
	return func(t *tablewriter.Table) {
		t.SetAlignment(alignment)
	}
}

// WithBorder controls border visibility
func WithBorder(show bool) RenderOption {
	return func(t *tablewriter.Table) {
		t.SetBorder(show)
	}
}

// WithAutoFormatHeaders enables/disables auto header formatting
func WithAutoFormatHeaders(enable bool) RenderOption {
	return func(t *tablewriter.Table) {
		t.SetAutoFormatHeaders(enable)
	}
}

// WithRowSeparator controls row separator lines
func WithRowSeparator(show bool) RenderOption {
	return func(t *tablewriter.Table) {
		t.SetRowSeparator("")
		if show {
			t.SetRowSeparator("-")
		}
	}
}

func (r *renderer) RenderToString(headers []string, rows [][]string, opts ...RenderOption) string {
	buf := &bytes.Buffer{}
	r.RenderToWriter(buf, headers, rows, opts...)
	return buf.String()
}

func (r *renderer) RenderToWriter(w io.Writer, headers []string, rows [][]string, opts ...RenderOption) {
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

// Compile-time interface compliance check
var _ Renderer = (*renderer)(nil)
