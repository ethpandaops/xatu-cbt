package database

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"
)

// memFS is a simple in-memory filesystem implementation for migration files.
// It implements the fs.FS interface required by golang-migrate's iofs source driver.
// This allows migrations to be served from memory without writing temporary files to disk,
// enabling safe parallel test execution with different database configurations.
type memFS struct {
	files map[string]string // Map of filename → SQL content
}

// Open opens a file or directory in the in-memory filesystem.
// Implements fs.FS interface required by golang-migrate.
func (m *memFS) Open(name string) (fs.File, error) {
	// Strip leading slash if present to normalize paths
	name = strings.TrimPrefix(name, "/")

	// Handle directory listing request (root directory)
	if name == "." || name == "" {
		return &memDir{fs: m}, nil
	}

	// Look up file content in memory
	content, ok := m.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}

	// Return an in-memory file handle
	return &memFile{
		name:    name,
		content: content,
		offset:  0,
	}, nil
}

// memFile represents a file in the in-memory filesystem.
// It implements fs.File interface to serve migration SQL content to golang-migrate.
type memFile struct {
	name    string // Filename (e.g., "001_create_tables.up.sql")
	content string // SQL content of the migration
	offset  int    // Current read position for implementing io.Reader
}

// Stat returns file information for this in-memory file.
// Implements fs.File interface.
func (f *memFile) Stat() (fs.FileInfo, error) {
	return &memFileInfo{
		name: f.name,
		size: int64(len(f.content)),
	}, nil
}

// Read reads data from the in-memory file into the provided byte slice.
// Implements io.Reader interface required by fs.File.
func (f *memFile) Read(p []byte) (int, error) {
	if f.offset >= len(f.content) {
		return 0, io.EOF
	}
	n := copy(p, f.content[f.offset:])
	f.offset += n
	if n > 0 && f.offset >= len(f.content) {
		return n, io.EOF
	}
	return n, nil
}

// Implements fs.File interface.
func (f *memFile) Close() error {
	return nil
}

// memDir represents a directory in the in-memory filesystem.
// It implements fs.ReadDirFile to provide directory listing functionality.
type memDir struct {
	fs      *memFS        // Reference to parent filesystem
	entries []fs.DirEntry // Cached list of directory entries
	offset  int           // Current position in entries list
}

// Stat returns file information for this directory.
// Implements fs.File interface.
func (d *memDir) Stat() (fs.FileInfo, error) {
	return &memFileInfo{
		name:  ".",
		size:  0,
		isDir: true,
	}, nil
}

// Read returns an error because directories cannot be read as files.
// Implements fs.File interface.
func (d *memDir) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("is a directory") //nolint:err113 // Standard error for directory read
}

// Implements fs.File interface.
func (d *memDir) Close() error {
	return nil
}

// ReadDir returns directory entries for the in-memory filesystem.
// If n <= 0, returns all remaining entries. Otherwise returns up to n entries.
// Implements fs.ReadDirFile interface required by golang-migrate.
func (d *memDir) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.entries == nil {
		// Build entries list on first call (lazy initialization)
		d.entries = make([]fs.DirEntry, 0, len(d.fs.files))
		for name := range d.fs.files {
			d.entries = append(d.entries, &memDirEntry{
				name: name,
				info: &memFileInfo{
					name: name,
					size: int64(len(d.fs.files[name])),
				},
			})
		}
	}

	if n <= 0 {
		// Return all remaining entries
		entries := d.entries[d.offset:]
		d.offset = len(d.entries)
		return entries, nil
	}

	// Return up to n entries
	end := d.offset + n
	if end > len(d.entries) {
		end = len(d.entries)
	}

	entries := d.entries[d.offset:end]
	d.offset = end

	return entries, nil
}

// memFileInfo implements fs.FileInfo for in-memory files and directories.
// Provides metadata about files in the memFS filesystem.
type memFileInfo struct {
	name  string // File or directory name
	size  int64  // Size in bytes (0 for directories)
	isDir bool   // Whether this represents a directory
}

// Name returns the base name of the file.
func (i *memFileInfo) Name() string { return i.name }

// Size returns the length in bytes for regular files.
func (i *memFileInfo) Size() int64 { return i.size }

// Mode returns the file mode bits.
// Returns 0o755 for directories, 0o644 for regular files.
func (i *memFileInfo) Mode() fs.FileMode {
	if i.isDir {
		return fs.ModeDir | 0o755
	}
	return 0o644
}

// ModTime returns the modification time (always zero for in-memory files).
func (i *memFileInfo) ModTime() time.Time { return time.Time{} }

// IsDir reports whether this describes a directory.
func (i *memFileInfo) IsDir() bool { return i.isDir }

// Sys returns the underlying data source (always nil for in-memory files).
func (i *memFileInfo) Sys() interface{} { return nil }

// memDirEntry implements fs.DirEntry for directory listings.
// Used by ReadDir to return information about files in the directory.
type memDirEntry struct {
	name string      // Entry name
	info fs.FileInfo // File information
}

// Name returns the name of the file described by the entry.
func (e *memDirEntry) Name() string { return e.name }

// IsDir reports whether the entry describes a directory.
func (e *memDirEntry) IsDir() bool { return e.info.IsDir() }

// Type returns the type bits for the entry.
func (e *memDirEntry) Type() fs.FileMode { return e.info.Mode().Type() }

// Info returns the FileInfo for the file or subdirectory described by the entry.
func (e *memDirEntry) Info() (fs.FileInfo, error) { return e.info, nil }
