// Package memfs provides a simple in-memory filesystem implementation for migration files.
package memfs

import (
	"io/fs"
	"os"
	"strings"
)

// FS is a simple in-memory filesystem implementation for migration files.
type FS struct {
	files map[string]string
}

// NewFS creates a new in-memory filesystem.
func NewFS() *FS {
	return &FS{
		files: make(map[string]string),
	}
}

// WriteFile adds or updates a file in the in-memory filesystem.
func (f *FS) WriteFile(name, content string) {
	f.files[name] = content
}

// ReadFile reads a file from the in-memory filesystem.
func (f *FS) ReadFile(name string) (string, bool) {
	content, ok := f.files[name]
	return content, ok
}

// Open opens a file or directory in the in-memory filesystem.
func (f *FS) Open(name string) (fs.File, error) {
	name = strings.TrimPrefix(name, "/")

	if name == "." || name == "" {
		return &Dir{fs: f}, nil
	}

	content, ok := f.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}

	return &File{
		name:    name,
		content: content,
		offset:  0,
	}, nil
}
