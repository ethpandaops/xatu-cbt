package memfs

import (
	"fmt"
	"io"
	"io/fs"
	"time"
)

// File represents a file in the in-memory filesystem.
type File struct {
	name    string
	content string
	offset  int
}

// Stat returns file information.
func (f *File) Stat() (fs.FileInfo, error) {
	return &fileInfo{
		name: f.name,
		size: int64(len(f.content)),
	}, nil
}

// Read reads data from the file.
func (f *File) Read(p []byte) (int, error) {
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

// Close implements fs.File interface.
func (f *File) Close() error {
	return nil
}

// Dir represents a directory in the in-memory filesystem.
type Dir struct {
	fs      *FS
	entries []fs.DirEntry
	offset  int
}

// Stat returns directory information.
func (d *Dir) Stat() (fs.FileInfo, error) {
	return &fileInfo{
		name:  ".",
		size:  0,
		isDir: true,
	}, nil
}

// Read returns an error for directories.
func (d *Dir) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("is a directory") //nolint:err113 // Standard error for directory read
}

// Close implements fs.File interface.
func (d *Dir) Close() error {
	return nil
}

// ReadDir returns directory entries.
func (d *Dir) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.entries == nil {
		d.entries = make([]fs.DirEntry, 0, len(d.fs.files))
		for name := range d.fs.files {
			d.entries = append(d.entries, &dirEntry{
				name: name,
				info: &fileInfo{
					name: name,
					size: int64(len(d.fs.files[name])),
				},
			})
		}
	}

	if n <= 0 {
		entries := d.entries[d.offset:]
		d.offset = len(d.entries)
		return entries, nil
	}

	end := d.offset + n
	if end > len(d.entries) {
		end = len(d.entries)
	}

	entries := d.entries[d.offset:end]
	d.offset = end

	return entries, nil
}

// fileInfo implements fs.FileInfo.
type fileInfo struct {
	name  string
	size  int64
	isDir bool
}

// Name returns the base name.
func (i *fileInfo) Name() string { return i.name }

// Size returns the file size.
func (i *fileInfo) Size() int64 { return i.size }

// Mode returns the file mode.
func (i *fileInfo) Mode() fs.FileMode {
	if i.isDir {
		return fs.ModeDir | 0o755
	}
	return 0o644
}

// ModTime returns the modification time.
func (i *fileInfo) ModTime() time.Time { return time.Time{} }

// IsDir reports whether this is a directory.
func (i *fileInfo) IsDir() bool { return i.isDir }

// Sys returns the underlying data source.
func (i *fileInfo) Sys() interface{} { return nil }

// dirEntry implements fs.DirEntry.
type dirEntry struct {
	name string
	info fs.FileInfo
}

// Name returns the entry name.
func (e *dirEntry) Name() string { return e.name }

// IsDir reports whether the entry is a directory.
func (e *dirEntry) IsDir() bool { return e.info.IsDir() }

// Type returns the type bits.
func (e *dirEntry) Type() fs.FileMode { return e.info.Mode().Type() }

// Info returns the FileInfo.
func (e *dirEntry) Info() (fs.FileInfo, error) { return e.info, nil }
