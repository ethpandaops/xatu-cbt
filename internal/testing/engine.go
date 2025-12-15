// Package testing provides end-to-end test orchestration and execution.
package testing

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver registration
	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// CBTEngine manages CBT engine lifecycle and transformation execution.
// This is the concrete implementation without an interface abstraction.
type CBTEngine struct {
	clickhouseURL string
	redisURL      string
	modelsDir     string
	externalDir   string
	log           logrus.FieldLogger
	config        *TestConfig
	modelCache    *ModelCache

	// Track running containers for cleanup
	runningContainers   []string
	runningContainersMu sync.Mutex

	// Pool of Redis DB numbers (1-15) for isolation between concurrent CBT containers.
	// Each CBT container gets its own Redis DB to prevent task queue conflicts.
	redisDBPool chan int
}

// Config represents the structure of the CBT YAML configuration file.
// This is the format expected by the CBT Docker container.
type cbtConfig struct {
	ClickHouse struct {
		URL         string `yaml:"url"`
		Cluster     string `yaml:"cluster,omitempty"`
		LocalSuffix string `yaml:"localSuffix,omitempty"`
		Admin       struct {
			Incremental struct {
				Database string `yaml:"database"`
				Table    string `yaml:"table"`
			} `yaml:"incremental"`
			Scheduled struct {
				Database string `yaml:"database"`
				Table    string `yaml:"table"`
			} `yaml:"scheduled"`
		} `yaml:"admin"`
	} `yaml:"clickhouse"`
	Redis struct {
		URL    string `yaml:"url"`
		Prefix string `yaml:"prefix"`
	} `yaml:"redis"`
	Models struct {
		External struct {
			DefaultCluster  string   `yaml:"defaultCluster,omitempty"`
			DefaultDatabase string   `yaml:"defaultDatabase,omitempty"`
			Paths           []string `yaml:"paths"`
		} `yaml:"external"`
		Transformations struct {
			DefaultDatabase string   `yaml:"defaultDatabase,omitempty"`
			Paths           []string `yaml:"paths"`
		} `yaml:"transformations"`
		Env       map[string]string          `yaml:"env,omitempty"`
		Overrides map[string]*modelOverrides `yaml:"overrides,omitempty"`
	} `yaml:"models"`
	Scheduler struct {
		Concurrency int `yaml:"concurrency"`
	} `yaml:"scheduler"`
	Worker struct {
		Concurrency int `yaml:"concurrency"`
	} `yaml:"worker"`
}

// modelOverrides contains per-model configuration overrides.
type modelOverrides struct {
	Config struct {
		Lag       *int              `yaml:"lag,omitempty"`
		Schedule  string            `yaml:"schedule,omitempty"`
		Schedules map[string]string `yaml:"schedules,omitempty"`
	} `yaml:"config"`
}

const (
	modelTypeScheduled   = "scheduled"
	modelTypeIncremental = "incremental"
)

// NewCBTEngine creates a new CBT engine manager.
func NewCBTEngine(
	log logrus.FieldLogger,
	cfg *TestConfig,
	modelCache *ModelCache,
	clickhouseURL,
	redisURL,
	modelsDir string,
) *CBTEngine {
	if cfg == nil {
		cfg = DefaultTestConfig()
	}

	// Create Redis DB pool with numbers 1-15 (max 15 concurrent, DB 0 reserved).
	// This ensures each concurrent CBT container gets its own Redis DB for task isolation.
	poolSize := cfg.CBTConcurrency
	if poolSize > 15 {
		poolSize = 15 // Redis only has DBs 0-15, reserve 0
	}

	redisDBPool := make(chan int, poolSize)
	for i := 1; i <= poolSize; i++ {
		redisDBPool <- i
	}

	return &CBTEngine{
		clickhouseURL: clickhouseURL,
		redisURL:      redisURL,
		modelsDir:     modelsDir,
		externalDir:   filepath.Join(modelsDir, "external"),
		log:           log.WithField("component", "cbt_engine"),
		config:        cfg,
		modelCache:    modelCache,
		redisDBPool:   redisDBPool,
	}
}

// Start initializes the CBT engine.
func (e *CBTEngine) Start(_ context.Context) error {
	e.log.Debug("starting cbt engine")
	return nil
}

// Stop cleans up the CBT engine resources.
func (e *CBTEngine) Stop() error {
	e.log.Debug("stopping cbt engine")

	// Kill all running containers
	e.runningContainersMu.Lock()
	containers := make([]string, len(e.runningContainers))
	copy(containers, e.runningContainers)
	e.runningContainersMu.Unlock()

	for _, containerName := range containers {
		e.log.WithField("container", containerName).Debug("cleaning up CBT container")

		killCmd := exec.Command("docker", "kill", containerName) //nolint:gosec // G204: Docker command with trusted container name
		_ = killCmd.Run()                                        // Ignore errors - container may already be stopped

		rmCmd := exec.Command("docker", "rm", "-f", containerName) //nolint:gosec // G204: Docker command with trusted container name
		if err := rmCmd.Run(); err != nil {
			e.log.WithError(err).WithField("container", containerName).Warn("failed to remove container")
		}
	}

	return nil
}

// flushRedisDB flushes a specific Redis database to clear stale data.
// This is called when acquiring a DB number from the pool to ensure clean state.
func (e *CBTEngine) flushRedisDB(ctx context.Context, dbNum int) error {
	// Use redis-cli -n to select the DB, then FLUSHDB to clear it.
	// dbNum is from a controlled pool (1-15), not user input.
	dbNumStr := fmt.Sprintf("%d", dbNum)
	args := []string{"exec", config.RedisContainerName, "redis-cli", "-n", dbNumStr, "FLUSHDB"}

	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // G204: args are from trusted internal pool

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("redis FLUSHDB failed: %w (output: %s)", err, string(output))
	}

	e.log.WithField("redisDB", dbNum).Debug("flushed redis database")

	return nil
}

// RunTransformations executes CBT transformations for specified models.
func (e *CBTEngine) RunTransformations(
	ctx context.Context,
	network,
	dbName,
	externalDB string,
	allModels,
	transformationModels []string,
) error {
	// Acquire a Redis DB number from the pool for isolation.
	// This ensures each concurrent CBT container uses a separate Redis database.
	var redisDB int
	select {
	case redisDB = <-e.redisDBPool:
		defer func() { e.redisDBPool <- redisDB }()
	case <-ctx.Done():
		return ctx.Err()
	}

	// Flush the Redis DB to clear any stale data from previous tests.
	// This is critical because DB numbers are reused from a pool.
	if err := e.flushRedisDB(ctx, redisDB); err != nil {
		return fmt.Errorf("flushing redis DB %d: %w", redisDB, err)
	}

	logCtx := e.log.WithFields(logrus.Fields{
		"database":   dbName,
		"externalDB": externalDB,
		"models":     len(transformationModels),
		"redisDB":    redisDB,
	})
	logCtx.Info("running transformations")

	start := time.Now()

	// Generate CBT config using all models (includes dependencies)
	tmpDir, err := os.MkdirTemp("", "cbt-config-*")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Use local variable for config path to avoid race condition.
	// Multiple concurrent tests share the same CBTEngine instance.
	configPath := filepath.Join(tmpDir, "config.yml")
	if err := e.generateConfig(network, dbName, externalDB, allModels, redisDB, configPath); err != nil {
		return fmt.Errorf("generating CBT config: %w", err)
	}

	// Execute CBT via docker, but only wait for test models
	if err := e.runDockerCBT(ctx, network, dbName, externalDB, transformationModels, configPath); err != nil {
		return fmt.Errorf("running CBT docker: %w", err)
	}

	logCtx.WithFields(logrus.Fields{
		"duration": time.Since(start),
	}).Info("transformations completed")

	return nil
}

// generateConfig generates a CBT config file for specific models.
func (e *CBTEngine) generateConfig(network, dbName, externalDB string, models []string, redisDB int, outputPath string) error {
	// Build model paths separated by type.
	externalPaths, transformationPaths, err := e.buildModelPaths(models)
	if err != nil {
		return fmt.Errorf("building model paths: %w", err)
	}

	cfg := &cbtConfig{}

	clickhouseHTTPURL := strings.Replace(e.clickhouseURL, "clickhouse://", "http://", 1)

	// Replace localhost with container hostname and adjust ports for Docker network access.
	containerEndpoint := fmt.Sprintf("%s:%s", config.ClickHouseContainer, config.ClickHouseContainerHTTPPort)
	clickhouseHTTPURL = strings.Replace(clickhouseHTTPURL, "localhost:9000", containerEndpoint, 1)
	clickhouseHTTPURL = strings.Replace(clickhouseHTTPURL, "localhost:8123", containerEndpoint, 1)
	cfg.ClickHouse.URL = clickhouseHTTPURL

	// Configure cluster settings for transformation models in CBT cluster
	cfg.ClickHouse.Cluster = config.CBTClusterName
	cfg.ClickHouse.LocalSuffix = config.ClickHouseLocalSuffix

	// Configure admin tables
	cfg.ClickHouse.Admin.Incremental.Database = dbName
	cfg.ClickHouse.Admin.Incremental.Table = "admin_cbt_incremental"
	cfg.ClickHouse.Admin.Scheduled.Database = dbName
	cfg.ClickHouse.Admin.Scheduled.Table = "admin_cbt_scheduled"

	// Configure Redis with isolated database per concurrent CBT container.
	// Each container gets its own Redis DB (1-15) to prevent task queue conflicts.
	redisContainerEndpoint := fmt.Sprintf("%s:%s", config.RedisContainerName, config.RedisContainerPort)
	redisURL := strings.Replace(e.redisURL, "localhost:6380", redisContainerEndpoint, 1)
	redisURL = strings.Replace(redisURL, "localhost:6379", redisContainerEndpoint, 1)
	cfg.Redis.URL = fmt.Sprintf("%s/%d", redisURL, redisDB)
	cfg.Redis.Prefix = fmt.Sprintf("test:%s:", dbName)

	// Set model paths and default databases
	cfg.Models.External.Paths = externalPaths
	cfg.Models.External.DefaultCluster = config.XatuClusterName
	cfg.Models.External.DefaultDatabase = externalDB
	cfg.Models.Transformations.Paths = transformationPaths
	cfg.Models.Transformations.DefaultDatabase = dbName

	// Set global environment variables
	cfg.Models.Env = map[string]string{
		"NETWORK":                                network,
		"EXTERNAL_MODEL_MIN_TIMESTAMP":           "0",
		"EXTERNAL_MODEL_MIN_BLOCK":               "0",
		"DATA_COLUMN_AVAILABILITY_LOOKBACK_DAYS": "3650", // 10 years for tests.
		"EXTERNAL_DATABASE":                      externalDB,
	}

	// Configure for fast test execution
	cfg.Scheduler.Concurrency = 10
	cfg.Worker.Concurrency = 10

	// Add test-optimized overrides
	cfg.Models.Overrides = e.buildTestOverrides(models)

	// Template config with database name
	e.templateConfig(cfg, dbName)

	// Write config to file
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(outputPath, data, 0o644); err != nil { //nolint:gosec // G306: Config file with standard permissions
		return fmt.Errorf("writing config file: %w", err)
	}

	e.log.WithFields(logrus.Fields{
		"external":        len(externalPaths),
		"transformations": len(transformationPaths),
	}).Debug("generated cbt config")

	return nil
}

// buildModelPaths converts model names to file paths for CBT, separated by type
func (e *CBTEngine) buildModelPaths(models []string) (externalPaths, transformationPaths []string, err error) {
	externalPaths = make([]string, 0)
	transformationPaths = make([]string, 0)

	transformationDir := filepath.Join(e.modelsDir, "transformations")

	for _, modelName := range models {
		// Try transformation directory first (.sql)
		transformPath := filepath.Join(transformationDir, modelName+".sql")
		if _, statErr := os.Stat(transformPath); statErr == nil {
			transformationPaths = append(transformationPaths, path.Join("/models/transformations", modelName+".sql"))
			continue
		}

		// Try transformation directory (.yml)
		transformYmlPath := filepath.Join(transformationDir, modelName+".yml")
		if _, statErr := os.Stat(transformYmlPath); statErr == nil {
			transformationPaths = append(transformationPaths, path.Join("/models/transformations", modelName+".yml"))
			continue
		}

		// Try external directory (.sql)
		externalPath := filepath.Join(e.externalDir, modelName+".sql")
		if _, statErr := os.Stat(externalPath); statErr == nil {
			externalPaths = append(externalPaths, path.Join("/models/external", modelName+".sql"))
			continue
		}

		// Try external directory (.yml)
		externalYmlPath := filepath.Join(e.externalDir, modelName+".yml")
		if _, statErr := os.Stat(externalYmlPath); statErr == nil {
			externalPaths = append(externalPaths, path.Join("/models/external", modelName+".yml"))
			continue
		}

		return nil, nil, fmt.Errorf("model %s not found in external or transformation directories", modelName) //nolint:err113 // Include model name for debugging
	}

	return externalPaths, transformationPaths, nil
}

// templateConfig applies database-specific templating to config
func (e *CBTEngine) templateConfig(cfg *cbtConfig, dbName string) {
	cfg.ClickHouse.URL = strings.ReplaceAll(cfg.ClickHouse.URL, "${NETWORK_NAME}", dbName)

	if !strings.Contains(cfg.Redis.Prefix, dbName) {
		e.log.WithField("database", dbName).Warn("Redis prefix does not contain database name")
	}
}

// buildTestOverrides creates test-optimized overrides by loading overrides.tests.yaml
func (e *CBTEngine) buildTestOverrides(models []string) map[string]*modelOverrides {
	overridesPath := "overrides.tests.yaml"
	data, err := os.ReadFile(overridesPath)
	if err != nil {
		e.log.WithError(err).Warn("failed to read overrides.tests.yaml, using default test overrides")
		return e.buildDefaultTestOverrides(models)
	}

	var overridesConfig struct {
		Models struct {
			Overrides map[string]*modelOverrides `yaml:"overrides"`
		} `yaml:"models"`
	}

	if err := yaml.Unmarshal(data, &overridesConfig); err != nil {
		e.log.WithError(err).Warn("failed to parse overrides.tests.yaml, using default test overrides")
		return e.buildDefaultTestOverrides(models)
	}

	e.log.WithFields(logrus.Fields{
		"from_file": len(overridesConfig.Models.Overrides),
	}).Info("applying overrides.tests.yaml")

	allOverrides := make(map[string]*modelOverrides)
	for modelName, override := range overridesConfig.Models.Overrides {
		allOverrides[modelName] = override
	}

	transformationDir := filepath.Join(e.modelsDir, "transformations")

	// Add default fast schedules for transformation models not in overrides file
	for _, modelName := range models {
		if allOverrides[modelName] != nil {
			continue
		}

		transformPath := filepath.Join(transformationDir, modelName+".sql")
		if _, err := os.Stat(transformPath); err == nil {
			allOverrides[modelName] = &modelOverrides{}
			allOverrides[modelName].Config.Schedules = map[string]string{
				"forwardfill": "@every 5s",
				"backfill":    "@every 5s",
			}
		}
	}

	return allOverrides
}

// buildDefaultTestOverrides creates fallback overrides
func (e *CBTEngine) buildDefaultTestOverrides(models []string) map[string]*modelOverrides {
	overrides := make(map[string]*modelOverrides)

	transformationDir := filepath.Join(e.modelsDir, "transformations")

	for _, modelName := range models {
		externalPath := filepath.Join(e.externalDir, modelName+".sql")
		if _, err := os.Stat(externalPath); err == nil {
			lag := 0
			overrides[modelName] = &modelOverrides{}
			overrides[modelName].Config.Lag = &lag
			continue
		}

		transformPath := filepath.Join(transformationDir, modelName+".sql")
		if _, err := os.Stat(transformPath); err == nil {
			overrides[modelName] = &modelOverrides{}
			overrides[modelName].Config.Schedule = "@every 5s"
			continue
		}
	}

	e.log.WithField("count", len(overrides)).Debug("generated default test overrides")

	return overrides
}

// runDockerCBT runs CBT in a docker container
func (e *CBTEngine) runDockerCBT(
	ctx context.Context,
	network,
	dbName,
	externalDB string,
	models []string,
	configPath string,
) error {
	e.log.WithFields(logrus.Fields{
		"network":  network,
		"database": dbName,
	}).Debug("starting cbt")

	absConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return fmt.Errorf("getting absolute config path: %w", err)
	}

	absModelsDir, err := filepath.Abs(e.modelsDir)
	if err != nil {
		return fmt.Errorf("getting absolute models path: %w", err)
	}

	// Use UnixNano to avoid container name collisions when tests start within the same second
	containerName := fmt.Sprintf("xatu-cbt-test-%d", time.Now().UnixNano())
	args := []string{
		"run",
		"--rm",
		"--name", containerName,
		"--network", e.config.DockerNetwork,
		"-e", fmt.Sprintf("NETWORK=%s", network),
		"-e", fmt.Sprintf("EXTERNAL_DATABASE=%s", externalDB),
		"-v", fmt.Sprintf("%s:/config/config.yml", absConfigPath),
		"-v", fmt.Sprintf("%s:/models", absModelsDir),
		e.config.DockerImage,
		"--config", "/config/config.yml",
	}

	execCtx, cancel := context.WithTimeout(ctx, e.config.ExecutionTimeout)
	defer cancel()

	cmd := exec.CommandContext(execCtx, "docker", args...) //nolint:gosec // G204: Docker command with controlled arguments

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting docker command: %w", err)
	}

	e.runningContainersMu.Lock()
	e.runningContainers = append(e.runningContainers, containerName)
	e.runningContainersMu.Unlock()

	defer func() {
		killCmd := exec.Command("docker", "kill", containerName) //nolint:gosec // G204: Docker cleanup with trusted container name
		_ = killCmd.Run()

		rmCmd := exec.Command("docker", "rm", "-f", containerName) //nolint:gosec // G204: Docker cleanup with trusted container name
		_ = rmCmd.Run()

		e.runningContainersMu.Lock()
		for i, name := range e.runningContainers {
			if name == containerName {
				e.runningContainers = append(e.runningContainers[:i], e.runningContainers[i+1:]...)
				break
			}
		}
		e.runningContainersMu.Unlock()
	}()

	if err := e.waitForTransformations(ctx, dbName, externalDB, models); err != nil {
		e.log.WithError(err).Warn("error waiting for transformations, continuing anyway")
	}

	return nil
}

// waitForTransformations polls admin tables with exponential backoff and verifies models have data
//
//nolint:gocyclo // Complex transformation waiting logic with multiple state checks
func (e *CBTEngine) waitForTransformations(ctx context.Context, dbName, externalDB string, models []string) error {
	scheduledModels := make(map[string]bool)
	allModels := make(map[string]bool)

	for _, model := range models {
		// Look up metadata from cache
		if !e.modelCache.IsTransformationModel(model) {
			// Skip external models - only wait for transformations
			continue
		}

		allModels[model] = true

		// Get metadata from cache (holds read lock)
		e.modelCache.mu.RLock()
		metadata := e.modelCache.transformationModels[model]
		e.modelCache.mu.RUnlock()

		// Check execution type from cached metadata
		if e.getModelType(metadata) == modelTypeScheduled {
			scheduledModels[model] = true
		}
	}

	if len(allModels) == 0 {
		e.log.Debug("no transformation models to wait for")
		return nil
	}

	e.log.WithFields(logrus.Fields{
		"total":     len(allModels),
		"scheduled": len(scheduledModels),
		"database":  dbName,
	}).Info("waiting for transformations to complete")

	conn, err := sql.Open("clickhouse", e.clickhouseURL)
	if err != nil {
		return fmt.Errorf("opening clickhouse connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	timeout := time.NewTimer(e.config.TransformationWaitTimeout)
	defer timeout.Stop()

	modelPendingSince := make(map[string]time.Time)
	scheduledRetryCount := 0
	const maxScheduledRetries = 3 // Allow up to 3 schedule cycles for scheduled models to catch up

	if err := e.waitForAdminTables(ctx, conn, dbName, timeout); err != nil {
		return fmt.Errorf("waiting for admin tables: %w", err)
	}

	// Build a map of scheduled models → their transitive incremental deps.
	// Used to verify deps have data before considering a scheduled model complete.
	scheduledModelIncrementalDeps := make(map[string][]string)
	for model := range scheduledModels {
		incrementalDeps := e.getIncrementalDependencies(model)
		if len(incrementalDeps) > 0 {
			scheduledModelIncrementalDeps[model] = incrementalDeps
		}
	}

	// Build a map of incremental models → their scheduled deps.
	// Used to verify scheduled deps have run before considering an incremental model complete.
	incrementalModelScheduledDeps := make(map[string][]string)
	for model := range allModels {
		if scheduledModels[model] {
			continue // Skip scheduled models
		}

		scheduledDeps := e.getScheduledDependencies(model)
		if len(scheduledDeps) > 0 {
			incrementalModelScheduledDeps[model] = scheduledDeps
		}
	}

	// Start with initial poll interval
	interval := e.config.InitialPollInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for transformations to complete") //nolint:err113 // Static timeout message
		case <-ticker.C:
			completedIncremental, err := e.getCompletedModels(ctx, conn, dbName, "admin_cbt_incremental")
			if err != nil {
				e.log.WithError(err).Debug("error checking incremental models")
				continue
			}

			completedScheduled, err := e.getCompletedModels(ctx, conn, dbName, "admin_cbt_scheduled")
			if err != nil {
				e.log.WithError(err).Debug("error checking scheduled models")
				continue
			}

			allCompleted := make(map[string]bool)
			for model := range completedIncremental {
				allCompleted[model] = true
			}
			for model := range completedScheduled {
				allCompleted[model] = true
			}

			pending := []string{}
			now := time.Now()

			for model := range allModels {
				if !allCompleted[model] { //nolint:nestif // Complex transformation tracking logic
					if _, exists := modelPendingSince[model]; !exists {
						modelPendingSince[model] = now
					}

					pendingDuration := now.Sub(modelPendingSince[model])

					if pendingDuration > e.config.PendingModelTimeout {
						tableExists, err := e.tableExists(ctx, conn, dbName, model)
						if err != nil {
							e.log.WithError(err).WithField("model", model).Debug("error checking table existence")
							pending = append(pending, model)
							continue
						}

						if tableExists {
							e.log.WithFields(logrus.Fields{
								"model":   model,
								"pending": pendingDuration,
							}).Info("model table exists but not in admin tables, assuming 0 rows")
							continue
						}
					}

					pending = append(pending, model)
					continue
				}

				tableExists, err := e.tableExists(ctx, conn, dbName, model)
				if err != nil {
					e.log.WithError(err).WithField("model", model).Debug("error checking table existence")
					pending = append(pending, model)
					continue
				}

				if !tableExists {
					e.log.WithField("model", model).Debug("table not created yet, waiting")
					pending = append(pending, model)
					continue
				}

				// For scheduled models, verify incremental deps have data before considering complete.
				// This prevents the race where a scheduled model runs before its incremental deps,
				// producing 0 rows because it read from empty tables.
				if incrementalDeps, hasIncrDeps := scheduledModelIncrementalDeps[model]; hasIncrDeps {
					allDepsHaveData := true
					for _, dep := range incrementalDeps {
						rows, err := e.getTableRowCount(ctx, conn, dbName, dep)
						if err != nil || rows == 0 {
							allDepsHaveData = false
							break
						}
					}
					if !allDepsHaveData {
						e.log.WithFields(logrus.Fields{
							"model": model,
							"deps":  incrementalDeps,
						}).Debug("scheduled model waiting for incremental deps to have data")
						pending = append(pending, model)
						continue
					}
				}

				// For incremental models, verify scheduled deps have run before considering complete.
				// CBT's bounds validation skips scheduled deps, so incremental models can run and
				// produce wrong results by reading from scheduled tables that haven't executed yet.
				// We check admin_cbt_scheduled (not row count) because scheduled models can
				// legitimately produce 0 rows.
				if scheduledDeps, hasSchedDeps := incrementalModelScheduledDeps[model]; hasSchedDeps {
					allDepsHaveRun := true
					for _, dep := range scheduledDeps {
						if !completedScheduled[dep] {
							allDepsHaveRun = false
							break
						}
					}
					if !allDepsHaveRun {
						e.log.WithFields(logrus.Fields{
							"model": model,
							"deps":  scheduledDeps,
						}).Debug("incremental model waiting for scheduled deps to run")
						pending = append(pending, model)
						continue
					}
				}

				// For incremental models only: check if model has 0 rows but deps have data.
				// This catches cases where an incremental model ran before deps had data.
				// We skip this check for scheduled models - they may legitimately produce 0 rows
				// based on their business logic (e.g., "last 365 days" queries with limited test data).
				if !scheduledModels[model] {
					modelRows, rowErr := e.getTableRowCount(ctx, conn, dbName, model)
					if rowErr == nil && modelRows == 0 {
						if e.modelHasDepsWithData(ctx, conn, dbName, externalDB, model) {
							e.log.WithField("model", model).Debug("incremental model has 0 rows but deps have data, waiting for re-run")
							pending = append(pending, model)
							continue
						}
					}
				}

				delete(modelPendingSince, model)
			}

			e.log.WithFields(logrus.Fields{
				"completed": fmt.Sprintf("[%v/%v]", len(allCompleted), len(allModels)),
				"pending":   pending,
			}).Debug("transformation progress")

			if len(pending) == 0 {
				// TEST SUITE ONLY: Check for models that might need another cycle.
				//
				// In production, CBT runs continuously and models execute on their schedules.
				// Dependencies are populated over time, so models find data when they run.
				//
				// In tests, we start CBT fresh with empty tables and load parquet data. There's a
				// race condition where a model's first execution fires before its dependencies
				// have processed data. The model runs, finds empty dependency tables, and produces
				// 0 rows.
				//
				// This retry logic detects when models produced 0 rows but their dependencies
				// now have data, and waits for the next execution cycle to re-run them.
				modelsNeedingRetry := e.findModelsNeedingRetry(ctx, conn, dbName, externalDB, allModels)
				if len(modelsNeedingRetry) > 0 && scheduledRetryCount < maxScheduledRetries {
					scheduledRetryCount++

					e.log.WithFields(logrus.Fields{
						"models": modelsNeedingRetry,
						"retry":  scheduledRetryCount,
						"max":    maxScheduledRetries,
					}).Debug("deps have data, waiting for another cycle")

					continue
				}

				return nil
			}

			// Apply exponential backoff to poll interval
			interval = time.Duration(float64(interval) * e.config.PollBackoffMultiplier)
			if interval > e.config.MaxPollInterval {
				interval = e.config.MaxPollInterval
			}
			ticker.Reset(interval)
		}
	}
}

// waitForAdminTables waits for CBT admin tables to be created
func (e *CBTEngine) waitForAdminTables(
	ctx context.Context,
	conn *sql.DB,
	dbName string,
	timeout *time.Timer,
) error {
	ticker := time.NewTicker(e.config.AdminTablePollInterval)
	defer ticker.Stop()

	adminTables := []string{"admin_cbt_incremental", "admin_cbt_scheduled"}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for admin tables to be created") //nolint:err113 // Static timeout message
		case <-ticker.C:
			allExist := true
			for _, tableName := range adminTables {
				exists, err := e.tableExists(ctx, conn, dbName, tableName)
				if err != nil {
					e.log.WithError(err).WithField("table", tableName).Debug("error checking admin table existence")
					allExist = false
					break
				}
				if !exists {
					e.log.WithField("table", tableName).Debug("admin table does not exist yet")
					allExist = false
					break
				}
			}

			if allExist {
				// Verify tables are queryable
				for _, tableName := range adminTables {
					query := fmt.Sprintf(`SELECT count() FROM %s.%s LIMIT 1`, dbName, tableName) //nolint:gosec // G201: Safe SQL with controlled identifiers
					var count uint64
					if err := conn.QueryRowContext(ctx, query).Scan(&count); err != nil {
						e.log.WithError(err).WithField("table", tableName).Debug("table exists but not queryable yet")
						allExist = false
						break
					}
				}

				if allExist {
					time.Sleep(2 * time.Second)
					return nil
				}
			}
		}
	}
}

// getCompletedModels returns models that have entries in the admin table
func (e *CBTEngine) getCompletedModels(
	ctx context.Context,
	conn *sql.DB,
	dbName,
	adminTable string,
) (map[string]bool, error) {
	query := fmt.Sprintf(`SELECT DISTINCT table FROM %s.%s`, dbName, adminTable) //nolint:gosec // G201: Safe SQL with controlled identifiers

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	completed := make(map[string]bool)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		completed[tableName] = true
	}

	return completed, nil
}

// tableExists checks if a table exists in the database
func (e *CBTEngine) tableExists(ctx context.Context, conn *sql.DB, dbName, tableName string) (bool, error) {
	query := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		`SELECT count()
		FROM system.tables
		WHERE database = '%s' AND name = '%s'`,
		dbName, tableName)

	var count uint64
	err := conn.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// findModelsNeedingRetry returns transformation models that have 0 rows but their
// dependencies have data. This indicates the model ran before its deps were ready
// and needs another execution cycle. Applies to both scheduled and incremental models.
func (e *CBTEngine) findModelsNeedingRetry(
	ctx context.Context,
	conn *sql.DB,
	dbName string,
	externalDB string,
	allModels map[string]bool,
) []string {
	needsRetry := []string{}

	for model := range allModels {
		// Check if this model has 0 rows
		modelRows, err := e.getTableRowCount(ctx, conn, dbName, model)
		if err != nil {
			e.log.WithError(err).WithField("model", model).Debug("error checking model row count")

			continue
		}

		if modelRows > 0 {
			// Model has data, no need to retry
			continue
		}

		// Model has 0 rows - check if any of its deps have data
		e.modelCache.mu.RLock()
		metadata := e.modelCache.transformationModels[model]
		e.modelCache.mu.RUnlock()

		if metadata == nil {
			continue
		}

		// Check all dependencies for data (both transformation and external)
		depsHaveData := false

		for _, dep := range metadata.Dependencies {
			var depRows uint64

			var checkErr error

			switch {
			case e.modelCache.IsTransformationModel(dep):
				// Transformation dep - check in CBT database
				depRows, checkErr = e.getTableRowCount(ctx, conn, dbName, dep)
			case e.modelCache.IsExternalModel(dep) && externalDB != "":
				// External dep - check in external database
				depRows, checkErr = e.getTableRowCount(ctx, conn, externalDB, dep)
			default:
				continue
			}

			if checkErr != nil {
				continue
			}

			if depRows > 0 {
				depsHaveData = true

				break
			}
		}

		// If deps have data but model has 0 rows, it needs another cycle
		if depsHaveData {
			needsRetry = append(needsRetry, model)
		}
	}

	return needsRetry
}

// modelHasDepsWithData checks if any of the model's dependencies have data.
// Used to determine if a model with 0 rows should be retried.
func (e *CBTEngine) modelHasDepsWithData(
	ctx context.Context,
	conn *sql.DB,
	dbName string,
	externalDB string,
	model string,
) bool {
	e.modelCache.mu.RLock()
	metadata := e.modelCache.transformationModels[model]
	e.modelCache.mu.RUnlock()

	if metadata == nil {
		return false
	}

	for _, dep := range metadata.Dependencies {
		var depRows uint64

		var checkErr error

		switch {
		case e.modelCache.IsTransformationModel(dep):
			// Transformation dep - check in CBT database
			depRows, checkErr = e.getTableRowCount(ctx, conn, dbName, dep)
		case e.modelCache.IsExternalModel(dep) && externalDB != "":
			// External dep - check in external database
			depRows, checkErr = e.getTableRowCount(ctx, conn, externalDB, dep)
		default:
			continue
		}

		if checkErr != nil {
			continue
		}

		if depRows > 0 {
			return true
		}
	}

	return false
}

// getTableRowCount returns the number of rows in a table
func (e *CBTEngine) getTableRowCount(ctx context.Context, conn *sql.DB, dbName, tableName string) (uint64, error) {
	query := fmt.Sprintf( //nolint:gosec // G201: Safe SQL with controlled identifiers
		"SELECT count() FROM `%s`.`%s` FINAL",
		dbName, tableName)

	var count uint64
	err := conn.QueryRowContext(ctx, query).Scan(&count)

	return count, err
}

// getModelType returns the execution type from cached model metadata.
// Returns modelTypeIncremental, modelTypeScheduled, or defaults to modelTypeScheduled.
func (e *CBTEngine) getModelType(metadata *ModelMetadata) string {
	if metadata == nil {
		e.log.Warn("nil metadata provided, assuming scheduled")
		return modelTypeScheduled
	}

	executionType := metadata.ExecutionType
	switch executionType {
	case "incremental":
		return modelTypeIncremental
	case "scheduled":
		return modelTypeScheduled
	default:
		// Default to scheduled for empty or unknown types
		return modelTypeScheduled
	}
}

// getScheduledDependencies returns scheduled model dependencies for a given incremental model.
// This is used to identify incremental models that depend on scheduled models,
// which require the scheduled deps to have run first.
func (e *CBTEngine) getScheduledDependencies(model string) []string {
	e.modelCache.mu.RLock()
	defer e.modelCache.mu.RUnlock()

	metadata := e.modelCache.transformationModels[model]
	if metadata == nil {
		return nil
	}

	// Only check incremental models - scheduled models don't have this race condition
	if e.getModelType(metadata) != modelTypeIncremental {
		return nil
	}

	scheduledDeps := make([]string, 0)

	for _, dep := range metadata.Dependencies {
		depMeta := e.modelCache.transformationModels[dep]
		if depMeta != nil && e.getModelType(depMeta) == modelTypeScheduled {
			scheduledDeps = append(scheduledDeps, dep)
		}
	}

	return scheduledDeps
}

// getIncrementalDependencies returns all incremental model dependencies (transitively) for a given scheduled model.
// This is used to identify scheduled models that depend on incremental models,
// which need their incremental deps to have data before the scheduled model can produce results.
// It recursively walks the dependency tree to find ALL incremental models, not just direct deps.
func (e *CBTEngine) getIncrementalDependencies(model string) []string {
	e.modelCache.mu.RLock()
	defer e.modelCache.mu.RUnlock()

	metadata := e.modelCache.transformationModels[model]
	if metadata == nil {
		return nil
	}

	// Only check scheduled models
	if e.getModelType(metadata) != modelTypeScheduled {
		return nil
	}

	// Use a set to track visited models and avoid cycles
	visited := make(map[string]bool)
	incrementalDeps := make([]string, 0)

	// Recursively collect all incremental deps
	e.collectIncrementalDepsRecursive(metadata.Dependencies, visited, &incrementalDeps)

	return incrementalDeps
}

// collectIncrementalDepsRecursive walks the dependency tree and collects all incremental models.
// Must be called with modelCache.mu already held (RLock).
func (e *CBTEngine) collectIncrementalDepsRecursive(deps []string, visited map[string]bool, result *[]string) {
	for _, dep := range deps {
		if visited[dep] {
			continue
		}

		visited[dep] = true

		depMeta := e.modelCache.transformationModels[dep]
		if depMeta == nil {
			continue // External model, skip
		}

		if e.getModelType(depMeta) == modelTypeIncremental {
			*result = append(*result, dep)
		}

		// Recursively check this model's dependencies
		e.collectIncrementalDepsRecursive(depMeta.Dependencies, visited, result)
	}
}
