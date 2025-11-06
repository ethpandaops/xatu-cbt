package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/sirupsen/logrus"
)

// ParquetCache manages local caching of parquet files.
type ParquetCache interface {
	Start(ctx context.Context) error
	Stop() error
	Get(ctx context.Context, url, tableName string) (string, error)
	Prefetch(ctx context.Context, urls map[string]string) error
	Cleanup() error
}

// Entry represents metadata for a cached file.
type Entry struct {
	URL        string    `json:"url"`
	SHA256     string    `json:"sha256"`
	Size       int64     `json:"size"`
	Downloaded time.Time `json:"downloaded"`
	LastUsed   time.Time `json:"last_used"`
	Table      string    `json:"table"`
}

// Manifest tracks all cached files.
type Manifest struct {
	Entries map[string]*Entry `json:"entries"` // Key: SHA256
}

type parquetCache struct {
	cacheDir     string
	maxSizeBytes int64
	httpClient   *http.Client
	log          logrus.FieldLogger
	metrics      metrics.Collector

	mu       sync.RWMutex
	manifest *Manifest

	// Concurrent download protection
	downloading sync.Map // URL → chan struct{}
}

const (
	manifestFilename       = "manifest.json"
	defaultHTTPTimeout     = 10 * time.Minute
	maxConcurrentDownloads = 10
)

// NewParquetCache creates a new parquet cache manager.
func NewParquetCache(log logrus.FieldLogger, cacheDir string, maxSizeBytes int64, metricsCollector metrics.Collector) ParquetCache {
	return &parquetCache{
		cacheDir:     cacheDir,
		maxSizeBytes: maxSizeBytes,
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
		log:      log.WithField("component", "parquet_cache"),
		metrics:  metricsCollector,
		manifest: &Manifest{Entries: make(map[string]*Entry)},
	}
}

func (c *parquetCache) Start(_ context.Context) error {
	if err := os.MkdirAll(c.cacheDir, 0o755); err != nil { //nolint:gosec // G301: Cache directory with standard permissions
		return fmt.Errorf("creating cache directory: %w", err)
	}

	if err := c.loadManifest(); err != nil {
		c.log.WithError(err).Warn("failed to load manifest, starting with empty cache")
		c.manifest = &Manifest{Entries: make(map[string]*Entry)}
	}

	c.log.WithFields(logrus.Fields{
		"entries":   len(c.manifest.Entries),
		"cache_dir": c.cacheDir,
	}).Info("parquet cache started")

	return nil
}

func (c *parquetCache) Stop() error {
	c.log.Debug("stopping parquet cache")

	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.saveManifest(); err != nil {
		return fmt.Errorf("saving manifest: %w", err)
	}

	return nil
}

func (c *parquetCache) Get(ctx context.Context, url, tableName string) (string, error) {
	startTime := time.Now()

	urlHash := c.hashURL(url)

	c.mu.RLock()
	_, exists := c.manifest.Entries[urlHash]
	c.mu.RUnlock()

	if exists {
		filePath := filepath.Join(c.cacheDir, urlHash)
		if fileInfo, err := os.Stat(filePath); err == nil {
			if err := c.updateLastUsed(urlHash); err != nil {
				c.log.WithError(err).Warn("failed to update last used time")
			}

			c.log.WithFields(logrus.Fields{
				"url":       url,
				"table":     tableName,
				"cache_hit": "true",
			}).Debug("fetching parquet file")

			c.metrics.RecordParquetLoad(metrics.ParquetLoadMetric{
				Table:     tableName,
				Source:    metrics.SourceCache,
				SizeBytes: fileInfo.Size(),
				Duration:  time.Since(startTime),
				Timestamp: time.Now(),
			})

			return filePath, nil
		}

		// File missing, remove from manifest
		c.mu.Lock()
		delete(c.manifest.Entries, urlHash)
		c.mu.Unlock()
	}

	c.log.WithFields(logrus.Fields{
		"url":       url,
		"table":     tableName,
		"cache_hit": exists,
	}).Debug("fetched parquet file")

	return c.download(ctx, url, urlHash, tableName)
}

// Prefetch downloads multiple files concurrently.
func (c *parquetCache) Prefetch(ctx context.Context, urls map[string]string) error {
	type job struct {
		url       string
		tableName string
	}

	jobs := make(chan job, len(urls))
	errors := make(chan error, len(urls))
	var wg sync.WaitGroup

	for i := 0; i < maxConcurrentDownloads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if _, err := c.Get(ctx, j.url, j.tableName); err != nil {
					errors <- fmt.Errorf("downloading %s: %w", j.tableName, err)
					return
				}
			}
		}()
	}

	for tableName, url := range urls {
		jobs <- job{url: url, tableName: tableName}
	}
	close(jobs)

	wg.Wait()
	close(errors)

	for err := range errors {
		return err
	}

	return nil
}

// Cleanup evicts old entries if cache size exceeds max.
func (c *parquetCache) Cleanup() error {
	c.log.Debug("running cache cleanup")

	manager := NewManager(c.cacheDir, c.log)
	currentSize := manager.GetCurrentSize(c.manifest)

	if currentSize > c.maxSizeBytes {
		c.log.WithFields(logrus.Fields{
			"current_size": currentSize,
			"max_size":     c.maxSizeBytes,
		}).Info("cache size exceeded, evicting old entries")

		deleted, err := manager.Evict(c.manifest, c.maxSizeBytes)
		if err != nil {
			return fmt.Errorf("evicting cache entries: %w", err)
		}

		c.log.WithField("deleted", len(deleted)).Info("evicted cache entries")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.saveManifest()
}

// download downloads a file and adds it to the cache
func (c *parquetCache) download(ctx context.Context, url, urlHash, tableName string) (string, error) {
	// Concurrent download protection
	downloadCh := make(chan struct{})
	actual, loaded := c.downloading.LoadOrStore(url, downloadCh)
	if loaded {
		// Another goroutine is downloading, wait for it
		select {
		case _, ok := <-actual.(chan struct{}): //nolint:errcheck // We're checking ok to determine channel closure
			if !ok {
				// Channel closed, download complete, retry Get
				return c.Get(ctx, url, tableName)
			}
			return c.Get(ctx, url, tableName)
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	defer func() {
		c.downloading.Delete(url)
		close(downloadCh)
	}()

	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("downloading file: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode) //nolint:err113 // Include status code for debugging
	}

	tmpPath := filepath.Join(c.cacheDir, urlHash+".tmp")
	tmpFile, err := os.Create(tmpPath) //nolint:gosec // G304: Path constructed from safe hash
	if err != nil {
		return "", fmt.Errorf("creating temp file: %w", err)
	}
	defer func() { _ = tmpFile.Close() }()

	hasher := sha256.New()
	writer := io.MultiWriter(tmpFile, hasher)

	written, err := io.Copy(writer, resp.Body)
	if err != nil {
		_ = os.Remove(tmpPath) // Ignore cleanup error, already returning an error
		return "", fmt.Errorf("writing file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath) // Ignore cleanup error, already returning an error
		return "", fmt.Errorf("closing temp file: %w", err)
	}

	sha256Hash := hex.EncodeToString(hasher.Sum(nil))

	finalPath := filepath.Join(c.cacheDir, urlHash)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath) // Ignore cleanup error, already returning an error
		return "", fmt.Errorf("moving file to cache: %w", err)
	}

	c.mu.Lock()
	c.manifest.Entries[urlHash] = &Entry{
		URL:        url,
		SHA256:     sha256Hash,
		Size:       written,
		Downloaded: time.Now(),
		LastUsed:   time.Now(),
		Table:      tableName,
	}

	if err := c.saveManifest(); err != nil {
		c.log.WithError(err).Warn("failed to save manifest after download")
	}
	c.mu.Unlock()

	duration := time.Since(start)

	c.log.WithFields(logrus.Fields{
		"url":      url,
		"size":     written,
		"duration": duration,
		"path":     finalPath,
	}).Debug("downloaded parquet file")

	// Record S3 download metric
	c.metrics.RecordParquetLoad(metrics.ParquetLoadMetric{
		Table:     tableName,
		Source:    metrics.SourceS3,
		SizeBytes: written,
		Duration:  duration,
		Timestamp: time.Now(),
	})

	return finalPath, nil
}

// updateLastUsed updates the last used timestamp for a cache entry
func (c *parquetCache) updateLastUsed(urlHash string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.manifest.Entries[urlHash]; ok {
		entry.LastUsed = time.Now()
	}

	return c.saveManifest()
}

// loadManifest loads the cache manifest from disk
func (c *parquetCache) loadManifest() error {
	manifestPath := filepath.Join(c.cacheDir, manifestFilename)

	data, err := os.ReadFile(manifestPath) //nolint:gosec // G304: Reading cache manifest from safe path
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No manifest yet
		}
		return fmt.Errorf("reading manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("parsing manifest: %w", err)
	}

	c.manifest = &manifest
	return nil
}

// saveManifest saves the cache manifest to disk
// Caller must hold at least a read lock (c.mu.RLock() or c.mu.Lock())
func (c *parquetCache) saveManifest() error {
	manifestPath := filepath.Join(c.cacheDir, manifestFilename)

	data, err := json.MarshalIndent(c.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}

	if err := os.WriteFile(manifestPath, data, 0o644); err != nil { //nolint:gosec // G306: Cache manifest with standard permissions
		return fmt.Errorf("writing manifest: %w", err)
	}

	return nil
}

// hashURL generates a SHA256 hash of the URL for cache key
func (c *parquetCache) hashURL(url string) string {
	hash := sha256.Sum256([]byte(url))
	return hex.EncodeToString(hash[:])
}
