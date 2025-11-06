// Package cache provides local disk caching with LRU eviction for parquet files.
package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/sirupsen/logrus"
)

// Manager handles cache eviction policies
type Manager interface {
	Evict(manifest *CacheManifest, maxSizeBytes int64) ([]string, error)
	GetCurrentSize(manifest *CacheManifest) int64
}

type manager struct {
	cacheDir string
	log      logrus.FieldLogger
}

// byLastUsed sorts cache entries by last used time (oldest first)
type byLastUsed []*cacheEntryWithKey

type cacheEntryWithKey struct {
	key   string
	entry *CacheEntry
}

// NewManager creates a new cache manager
func NewManager(cacheDir string, log logrus.FieldLogger) Manager {
	return &manager{
		cacheDir: cacheDir,
		log:      log.WithField("component", "cache_manager"),
	}
}

// GetCurrentSize calculates the total size of all cached files
func (m *manager) GetCurrentSize(manifest *CacheManifest) int64 {
	var totalSize int64
	for _, entry := range manifest.Entries {
		totalSize += entry.Size
	}
	return totalSize
}

// Evict performs LRU eviction until cache size is under maxSizeBytes
func (m *manager) Evict(manifest *CacheManifest, maxSizeBytes int64) ([]string, error) {
	currentSize := m.GetCurrentSize(manifest)

	m.log.WithFields(logrus.Fields{
		"current_size": currentSize,
		"max_size":     maxSizeBytes,
	}).Debug("starting cache eviction")

	if currentSize <= maxSizeBytes {
		m.log.Debug("cache size under limit, no eviction needed")
		return nil, nil
	}

	// Build sortable list of entries
	entries := make([]*cacheEntryWithKey, 0, len(manifest.Entries))
	for key, entry := range manifest.Entries {
		entries = append(entries, &cacheEntryWithKey{
			key:   key,
			entry: entry,
		})
	}

	// Sort by last used time (oldest first)
	sort.Sort(byLastUsed(entries))

	// Evict oldest entries until under size limit
	deleted := make([]string, 0)
	var sizeFreed int64

	for _, item := range entries {
		if currentSize-sizeFreed <= maxSizeBytes {
			break
		}

		// Delete file from disk
		filePath := filepath.Join(m.cacheDir, item.key)
		if err := os.Remove(filePath); err != nil {
			if !os.IsNotExist(err) {
				m.log.WithError(err).WithField("file", filePath).Warn("failed to delete cache file")
			}
		} else {
			m.log.WithFields(logrus.Fields{
				"file":  item.entry.Table,
				"size":  item.entry.Size,
				"age":   item.entry.LastUsed,
			}).Debug("evicted cache entry")
		}

		// Remove from manifest
		delete(manifest.Entries, item.key)
		deleted = append(deleted, item.key)
		sizeFreed += item.entry.Size
	}

	newSize := currentSize - sizeFreed
	m.log.WithFields(logrus.Fields{
		"evicted":  len(deleted),
		"freed":    sizeFreed,
		"new_size": newSize,
	}).Info("cache eviction complete")

	if newSize > maxSizeBytes {
		return deleted, fmt.Errorf("unable to free enough space: current=%d, max=%d", newSize, maxSizeBytes) //nolint:err113 // Include size values for debugging
	}

	return deleted, nil
}

// Len implements sort.Interface
func (b byLastUsed) Len() int {
	return len(b)
}

// Less implements sort.Interface (oldest first)
func (b byLastUsed) Less(i, j int) bool {
	return b[i].entry.LastUsed.Before(b[j].entry.LastUsed)
}

// Swap implements sort.Interface
func (b byLastUsed) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}
