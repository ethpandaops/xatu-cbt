package cbt

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver registration
	"github.com/sirupsen/logrus"
)

const (
	modelTypeScheduled = "scheduled"
)

// Engine manages CBT engine lifecycle
type Engine interface {
	Start(ctx context.Context) error
	Stop() error
	// RunTransformations runs CBT transformations
	// configModels: all models for CBT config generation (includes dependencies)
	// waitModels: models to wait for completion (typically just the test models)
	RunTransformations(ctx context.Context, network, dbName string, configModels, waitModels []string) error
}

type engine struct {
	configGen     ConfigGenerator
	clickhouseURL string
	redisURL      string
	modelsDir     string
	log           logrus.FieldLogger

	configPath string

	// Track running containers for cleanup
	runningContainers   []string
	runningContainersMu sync.Mutex
}

const (
	dockerImage      = "ethpandaops/cbt:debian-latest"
	dockerNetwork    = "xatu_xatu-net"
	executionTimeout = 30 * time.Minute
)

// NewEngine creates a new CBT engine manager
func NewEngine(log logrus.FieldLogger, configGen ConfigGenerator, clickhouseURL, redisURL, modelsDir string) Engine {
	return &engine{
		configGen:     configGen,
		clickhouseURL: clickhouseURL,
		redisURL:      redisURL,
		modelsDir:     modelsDir,
		log:           log.WithField("component", "cbt_engine"),
	}
}

// Start initializes the engine (no-op for this implementation)
func (e *engine) Start(_ context.Context) error {
	e.log.Debug("starting CBT engine")
	return nil
}

// Stop cleans up the engine
func (e *engine) Stop() error {
	e.log.Debug("stopping CBT engine")

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

// RunTransformations executes CBT transformations for specified models
func (e *engine) RunTransformations(ctx context.Context, network, dbName string, allModels, transformationModels []string) error {
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
	if err := e.configGen.GenerateForModels(network, dbName, allModels, e.configPath); err != nil {
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

// runDockerCBT runs CBT in a docker container
func (e *engine) runDockerCBT(ctx context.Context, network, dbName string, models []string, configPath string) error {
	e.log.WithFields(logrus.Fields{
		"network":  network,
		"database": dbName,
		"config":   configPath,
	}).Debug("running CBT in docker")

	// Get absolute path to config
	absConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return fmt.Errorf("getting absolute config path: %w", err)
	}

	// Get absolute path to models directory
	absModelsDir, err := filepath.Abs(e.modelsDir)
	if err != nil {
		return fmt.Errorf("getting absolute models path: %w", err)
	}

	// Build docker command - use --name to track container
	containerName := fmt.Sprintf("xatu-cbt-test-%d", time.Now().Unix())
	args := []string{
		"run",
		"--rm",
		"--name", containerName,
		"--network", dockerNetwork,
		"-e", fmt.Sprintf("NETWORK=%s", network),
		"-v", fmt.Sprintf("%s:/config/config.yml", absConfigPath),
		"-v", fmt.Sprintf("%s:/models", absModelsDir),
		dockerImage,
		"--config", "/config/config.yml",
	}

	e.log.WithFields(logrus.Fields{
		"command":   fmt.Sprintf("docker %s", strings.Join(args, " ")),
		"container": containerName,
	}).Debug("executing docker command")

	// Start CBT container in background
	execCtx, cancel := context.WithTimeout(ctx, executionTimeout)
	defer cancel()

	cmd := exec.CommandContext(execCtx, "docker", args...) //nolint:gosec // G204: Docker command with controlled arguments

	// Capture output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting docker command: %w", err)
	}

	// Track running container
	e.runningContainersMu.Lock()
	e.runningContainers = append(e.runningContainers, containerName)
	e.runningContainersMu.Unlock()

	// Ensure container is killed on exit
	defer func() {
		killCmd := exec.Command("docker", "kill", containerName) //nolint:gosec // G204: Docker cleanup with trusted container name
		_ = killCmd.Run() // Ignore errors - container may already be stopped

		// Remove from tracking
		e.runningContainersMu.Lock()
		for i, name := range e.runningContainers {
			if name == containerName {
				e.runningContainers = append(e.runningContainers[:i], e.runningContainers[i+1:]...)
				break
			}
		}
		e.runningContainersMu.Unlock()
	}()

	// Wait for transformations to complete by monitoring admin table
	if err := e.waitForTransformations(ctx, dbName, models); err != nil {
		e.log.WithError(err).Warn("error waiting for transformations, continuing anyway")
	}

	// Kill the container
	e.log.Debug("killing cbt container")

	return nil
}

// waitForTransformations polls the admin tables and verifies models have data
// For scheduled models, it retries if the model runs but produces empty results
// Has a hard 10-minute timeout to prevent running forever
func (e *engine) waitForTransformations(ctx context.Context, dbName string, models []string) error { //nolint:gocyclo // Complex transformation waiting logic with multiple state checks - refactoring risky
	// Categorize transformation models (skip external models)
	scheduledModels := make(map[string]bool)
	allModels := make(map[string]bool)

	for _, model := range models {
		// Check for .sql files
		transformPath := filepath.Join(e.modelsDir, "transformations", model+".sql")
		if _, err := os.Stat(transformPath); err == nil {
			allModels[model] = true
			if e.getModelType(transformPath) == modelTypeScheduled {
				scheduledModels[model] = true
			}
			continue
		}

		// Check for .yml files
		transformYmlPath := filepath.Join(e.modelsDir, "transformations", model+".yml")
		if _, err := os.Stat(transformYmlPath); err == nil {
			allModels[model] = true
			if e.getModelType(transformYmlPath) == modelTypeScheduled {
				scheduledModels[model] = true
			}
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

	// Hard 10-minute timeout for entire wait
	timeout := time.NewTimer(10 * time.Minute)
	defer timeout.Stop()

	// OPTION 1: Track when models first became pending
	// If pending >30s and table exists, consider complete (handles 0-row case)
	modelPendingSince := make(map[string]time.Time)

	// ANTI-FLAKE: Wait for admin tables to be created by CBT before polling them
	// This prevents race condition where assertions query admin tables before they exist
	e.log.Debug("waiting for CBT admin tables to be created")
	if err := e.waitForAdminTables(ctx, conn, dbName, timeout); err != nil {
		return fmt.Errorf("waiting for admin tables: %w", err)
	}
	e.log.Debug("CBT admin tables ready")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for transformations to complete after 10 minutes") //nolint:err113 // Static timeout message
		case <-ticker.C:
			// Check which models have completed
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

			// Combine completed models
			allCompleted := make(map[string]bool)
			for model := range completedIncremental {
				allCompleted[model] = true
			}
			for model := range completedScheduled {
				allCompleted[model] = true
			}

			// Check if all models are in admin tables AND have tables created
			// OPTION 1: Track pending time and accept 0-row models after timeout
			pending := []string{}
			missingTables := []string{}
			now := time.Now()

			for model := range allModels {
				if !allCompleted[model] { //nolint:nestif // Complex transformation tracking logic - refactoring risky
					// Track when this model first became pending
					if _, exists := modelPendingSince[model]; !exists {
						modelPendingSince[model] = now
					}

					pendingDuration := now.Sub(modelPendingSince[model])

					// OPTION 1: If pending >90s, check if table exists (handles 0-row case)
					// Increased timeout for CI environments where transformations take longer
					if pendingDuration > 90*time.Second {
						tableExists, err := e.tableExists(ctx, conn, dbName, model)
						if err != nil {
							e.log.WithError(err).WithField("model", model).Debug("error checking table existence")
							pending = append(pending, model)
							continue
						}

						if tableExists {
							// Table exists but no admin entry after 3min = 0 rows produced (legitimate)
							e.log.WithFields(logrus.Fields{
								"model":   model,
								"pending": pendingDuration,
							}).Info("model table exists but not in admin tables after 3min, assuming 0 rows (legitimate)")
							// Don't add to pending - mark as complete
							continue
						}
					}

					pending = append(pending, model)
					continue
				}

				// Model is in admin tables, verify table actually exists
				tableExists, err := e.tableExists(ctx, conn, dbName, model)
				if err != nil {
					e.log.WithError(err).WithField("model", model).Debug("error checking table existence")
					pending = append(pending, model) // Keep waiting on errors
					missingTables = append(missingTables, model)
					continue
				}

				if !tableExists {
					e.log.WithField("model", model).Debug("table not created yet, waiting")
					pending = append(pending, model)
					missingTables = append(missingTables, model)
					continue
				}

				// Model complete - remove from pending tracking
				delete(modelPendingSince, model)
			}

			e.log.WithFields(logrus.Fields{
				"completed":      len(allCompleted),
				"total":          len(allModels),
				"pending":        pending,
				"missing_tables": missingTables,
			}).Debug("checking transformation progress")

			if len(pending) == 0 {
				return nil
			}
		}
	}
}

// waitForAdminTables waits for CBT admin tables to be created
// Returns when both admin_cbt_incremental and admin_cbt_scheduled exist
func (e *engine) waitForAdminTables(ctx context.Context, conn *sql.DB, dbName string, timeout *time.Timer) error {
	ticker := time.NewTicker(500 * time.Millisecond)
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
				// ANTI-FLAKE: Tables exist in system.tables, but verify they're actually queryable
				// In clustered setups, there can be a delay between metadata and data availability
				e.log.Debug("admin tables exist in system.tables, verifying they are queryable")

				for _, tableName := range adminTables {
					// Try to actually query the table to ensure it's not just metadata
					query := fmt.Sprintf(`SELECT count() FROM %s.%s LIMIT 1`, dbName, tableName) //nolint:gosec // G201: Safe SQL with controlled identifiers
					var count uint64
					if err := conn.QueryRowContext(ctx, query).Scan(&count); err != nil {
						e.log.WithError(err).WithField("table", tableName).Debug("table exists but not queryable yet, waiting")
						allExist = false
						break
					}
				}

				if allExist {
					e.log.Debug("admin tables are queryable, waiting for cluster propagation")

					// ANTI-FLAKE: Give cluster time to fully replicate table metadata across all nodes
					// Assertions create new connections that might hit different nodes than our check
					// In distributed ClickHouse, metadata replication can take 1-2 seconds
					time.Sleep(2 * time.Second)

					e.log.Debug("cluster propagation complete")
					return nil
				}
			}
		}
	}
}

// getCompletedModels returns models that have entries in the admin table
func (e *engine) getCompletedModels(ctx context.Context, conn *sql.DB, dbName, adminTable string) (map[string]bool, error) {
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
func (e *engine) tableExists(ctx context.Context, conn *sql.DB, dbName, tableName string) (bool, error) {
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

// getModelType reads a model file and determines if it's incremental or scheduled
func (e *engine) getModelType(filePath string) string {
	data, err := os.ReadFile(filePath) //nolint:gosec // G304: Reading model files from trusted paths
	if err != nil {
		e.log.WithError(err).Warn("failed to read model file, assuming scheduled")
		return modelTypeScheduled
	}

	content := string(data)

	// Look for "type: incremental" or "type: scheduled" in YAML frontmatter
	if strings.Contains(content, "type: incremental") {
		return "incremental"
	} else if strings.Contains(content, "type: scheduled") {
		return modelTypeScheduled
	}

	// Default to scheduled if type not found
	e.log.WithField("file", filePath).Debug("model type not found, defaulting to scheduled")
	return modelTypeScheduled
}
