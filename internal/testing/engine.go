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

	configPath string

	// Track running containers for cleanup
	runningContainers   []string
	runningContainersMu sync.Mutex
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

	return &CBTEngine{
		clickhouseURL: clickhouseURL,
		redisURL:      redisURL,
		modelsDir:     modelsDir,
		externalDir:   filepath.Join(modelsDir, "external"),
		log:           log.WithField("component", "cbt_engine"),
		config:        cfg,
		modelCache:    modelCache,
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
		e.log.WithField("container", containerName).Debug("killing CBT container")
		killCmd := exec.Command("docker", "kill", containerName) //nolint:gosec // G204: Docker command with trusted container name
		if err := killCmd.Run(); err != nil {
			e.log.WithError(err).WithField("container", containerName).Warn("failed to kill container")
		}
	}

	// Clean up config file if it exists
	if e.configPath != "" {
		if err := os.Remove(e.configPath); err != nil && !os.IsNotExist(err) {
			e.log.WithError(err).Warn("failed to remove config file")
		}
	}

	return nil
}

// RunTransformations executes CBT transformations for specified models.
func (e *CBTEngine) RunTransformations(
	ctx context.Context,
	network,
	dbName string,
	allModels,
	transformationModels []string,
) error {
	logCtx := e.log.WithFields(logrus.Fields{
		"database": dbName,
		"models":   len(transformationModels),
	})
	logCtx.Info("running transformations")

	start := time.Now()

	// Generate CBT config using all models (includes dependencies)
	tmpDir, err := os.MkdirTemp("", "cbt-config-*")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	e.configPath = filepath.Join(tmpDir, "config.yml")
	if err := e.generateConfig(network, dbName, allModels, e.configPath); err != nil {
		return fmt.Errorf("generating CBT config: %w", err)
	}

	// Execute CBT via docker, but only wait for test models
	if err := e.runDockerCBT(ctx, network, dbName, transformationModels, e.configPath); err != nil {
		return fmt.Errorf("running CBT docker: %w", err)
	}

	logCtx.WithFields(logrus.Fields{
		"duration": time.Since(start),
	}).Info("transformations completed")

	return nil
}

// generateConfig generates a CBT config file for specific models.
func (e *CBTEngine) generateConfig(network, dbName string, models []string, outputPath string) error {
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

	// Configure Redis with namespace per test database
	redisContainerEndpoint := fmt.Sprintf("%s:%s", config.RedisContainerName, config.RedisContainerPort)
	redisURL := strings.Replace(e.redisURL, "localhost:6380", redisContainerEndpoint, 1)
	redisURL = strings.Replace(redisURL, "localhost:6379", redisContainerEndpoint, 1)
	cfg.Redis.URL = redisURL
	cfg.Redis.Prefix = fmt.Sprintf("test:%s:", dbName)

	// Set model paths and default databases
	cfg.Models.External.Paths = externalPaths
	cfg.Models.External.DefaultCluster = config.XatuClusterName
	cfg.Models.External.DefaultDatabase = config.DefaultDatabase
	cfg.Models.Transformations.Paths = transformationPaths
	cfg.Models.Transformations.DefaultDatabase = dbName

	// Set global environment variables
	cfg.Models.Env = map[string]string{
		"NETWORK":                                network,
		"EXTERNAL_MODEL_MIN_TIMESTAMP":           "0",
		"EXTERNAL_MODEL_MIN_BLOCK":               "0",
		"DATA_COLUMN_AVAILABILITY_LOOKBACK_DAYS": "3650", // 10 years for tests.
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
		"output":          outputPath,
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
	dbName string,
	models []string,
	configPath string,
) error {
	e.log.WithFields(logrus.Fields{
		"network":  network,
		"database": dbName,
		"config":   configPath,
	}).Debug("starting cbt")

	absConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return fmt.Errorf("getting absolute config path: %w", err)
	}

	absModelsDir, err := filepath.Abs(e.modelsDir)
	if err != nil {
		return fmt.Errorf("getting absolute models path: %w", err)
	}

	containerName := fmt.Sprintf("xatu-cbt-test-%d", time.Now().Unix())
	args := []string{
		"run",
		"--rm",
		"--name", containerName,
		"--network", e.config.DockerNetwork,
		"-e", fmt.Sprintf("NETWORK=%s", network),
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

		e.runningContainersMu.Lock()
		for i, name := range e.runningContainers {
			if name == containerName {
				e.runningContainers = append(e.runningContainers[:i], e.runningContainers[i+1:]...)
				break
			}
		}
		e.runningContainersMu.Unlock()
	}()

	if err := e.waitForTransformations(ctx, dbName, models); err != nil {
		e.log.WithError(err).Warn("error waiting for transformations, continuing anyway")
	}

	return nil
}

// waitForTransformations polls admin tables with exponential backoff and verifies models have data
//
//nolint:gocyclo // Complex transformation waiting logic with multiple state checks
func (e *CBTEngine) waitForTransformations(ctx context.Context, dbName string, models []string) error {
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

	if err := e.waitForAdminTables(ctx, conn, dbName, timeout); err != nil {
		return fmt.Errorf("waiting for admin tables: %w", err)
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

				delete(modelPendingSince, model)
			}

			e.log.WithFields(logrus.Fields{
				"completed": fmt.Sprintf("[%v/%v]", len(allCompleted), len(allModels)),
				"pending":   pending,
			}).Debug("transformation progress")

			if len(pending) == 0 {
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
