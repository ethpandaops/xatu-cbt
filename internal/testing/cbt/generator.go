// Package cbt provides CBT configuration generation and transformation execution.
package cbt

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// ConfigGenerator generates CBT config files.
type ConfigGenerator interface {
	GenerateForModels(network, dbName string, models []string, outputPath string) error
}

type configGenerator struct {
	externalDir       string
	transformationDir string
	clickhouseURL     string
	redisURL          string
	log               logrus.FieldLogger
}

// NewConfigGenerator creates a new CBT config generator.
func NewConfigGenerator(
	log logrus.FieldLogger,
	externalDir,
	transformationDir,
	clickhouseURL,
	redisURL string,
) ConfigGenerator {
	return &configGenerator{
		externalDir:       externalDir,
		transformationDir: transformationDir,
		clickhouseURL:     clickhouseURL,
		redisURL:          redisURL,
		log:               log.WithField("component", "cbt_config_generator"),
	}
}

// GenerateForModels generates a CBT config for specific models.
func (g *configGenerator) GenerateForModels(network, dbName string, models []string, outputPath string) error {
	// Build model paths separated by type.
	externalPaths, transformationPaths, err := g.buildModelPaths(models)
	if err != nil {
		return fmt.Errorf("building model paths: %w", err)
	}

	cfg := &Config{}

	clickhouseHTTPURL := strings.Replace(g.clickhouseURL, "clickhouse://", "http://", 1)

	// Replace localhost with container hostname and adjust ports for Docker network access.
	// Host ports (9000 native, 8123 HTTP) are mapped differently than container internal ports.
	containerEndpoint := fmt.Sprintf("%s:%s", config.ClickHouseContainer, config.ClickHouseContainerHTTPPort)
	clickhouseHTTPURL = strings.Replace(clickhouseHTTPURL, "localhost:9000", containerEndpoint, 1)
	clickhouseHTTPURL = strings.Replace(clickhouseHTTPURL, "localhost:8123", containerEndpoint, 1)
	cfg.ClickHouse.URL = clickhouseHTTPURL

	// Configure cluster settings for transformation models in CBT cluster
	// Test databases are created ON CLUSTER using CBT cluster macros
	// This enables DELETE to work on ReplicatedReplacingMergeTree tables using _local suffix
	// External models override this with their own defaultCluster
	cfg.ClickHouse.Cluster = config.CBTClusterName
	cfg.ClickHouse.LocalSuffix = config.ClickHouseLocalSuffix

	// Configure admin tables - these are created by migrations in the test database
	cfg.ClickHouse.Admin.Incremental.Database = dbName
	cfg.ClickHouse.Admin.Incremental.Table = "admin_cbt_incremental"
	cfg.ClickHouse.Admin.Scheduled.Database = dbName
	cfg.ClickHouse.Admin.Scheduled.Table = "admin_cbt_scheduled"

	// Configure Redis with namespace per test database
	// ANTI-FLAKE #5: This prevents state leakage between concurrent tests
	// Replace localhost with container hostname and adjust port for Docker network access
	// Host port 6380 maps to container internal port 6379
	redisContainerEndpoint := fmt.Sprintf("%s:%s", config.RedisContainerName, config.RedisContainerPort)
	redisURL := strings.Replace(g.redisURL, "localhost:6380", redisContainerEndpoint, 1)
	redisURL = strings.Replace(redisURL, "localhost:6379", redisContainerEndpoint, 1)
	cfg.Redis.URL = redisURL
	cfg.Redis.Prefix = fmt.Sprintf("test:%s:", dbName)

	// Set model paths and default databases
	cfg.Models.External.Paths = externalPaths
	cfg.Models.External.DefaultCluster = config.XatuClusterName        // External data is on remote xatu cluster
	cfg.Models.External.DefaultDatabase = config.DefaultDatabase       // External data is in default database
	cfg.Models.Transformations.Paths = transformationPaths
	cfg.Models.Transformations.DefaultDatabase = dbName // Transformations use test database

	// Set global environment variables for template rendering
	// NETWORK is used in external model queries: WHERE meta_network_name = '{{ .env.NETWORK }}'
	cfg.Models.Env = map[string]string{
		"NETWORK": network,
	}

	// Configure for fast test execution
	cfg.Scheduler.Concurrency = 10
	cfg.Worker.Concurrency = 10

	// Add test-optimized overrides for all models
	cfg.Models.Overrides = g.buildTestOverrides(models)

	// Template config with database name
	g.templateConfig(cfg, dbName)

	// Write config to file
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(outputPath, data, 0o644); err != nil { //nolint:gosec // G306: Config file with standard permissions
		return fmt.Errorf("writing config file: %w", err)
	}

	g.log.WithFields(logrus.Fields{
		"external":        len(externalPaths),
		"transformations": len(transformationPaths),
		"output":          outputPath,
	}).Debug("generated cbt config")

	return nil
}

// buildModelPaths converts model names to file paths for CBT, separated by type
func (g *configGenerator) buildModelPaths(models []string) (externalPaths, transformationPaths []string, err error) {
	externalPaths = make([]string, 0)
	transformationPaths = make([]string, 0)

	for _, modelName := range models {
		// Try transformation directory first (.sql)
		transformPath := filepath.Join(g.transformationDir, modelName+".sql")
		if _, statErr := os.Stat(transformPath); statErr == nil {
			// Use container path
			transformationPaths = append(transformationPaths, path.Join("/models/transformations", modelName+".sql"))
			continue
		}

		// Try transformation directory (.yml)
		transformYmlPath := filepath.Join(g.transformationDir, modelName+".yml")
		if _, statErr := os.Stat(transformYmlPath); statErr == nil {
			// Use container path
			transformationPaths = append(transformationPaths, path.Join("/models/transformations", modelName+".yml"))
			continue
		}

		// Try external directory (.sql)
		externalPath := filepath.Join(g.externalDir, modelName+".sql")
		if _, statErr := os.Stat(externalPath); statErr == nil {
			// Use container path
			externalPaths = append(externalPaths, path.Join("/models/external", modelName+".sql"))
			continue
		}

		// Try external directory (.yml)
		externalYmlPath := filepath.Join(g.externalDir, modelName+".yml")
		if _, statErr := os.Stat(externalYmlPath); statErr == nil {
			// Use container path
			externalPaths = append(externalPaths, path.Join("/models/external", modelName+".yml"))
			continue
		}

		return nil, nil, fmt.Errorf("model %s not found in external or transformation directories", modelName) //nolint:err113 // Include model name for debugging
	}

	return externalPaths, transformationPaths, nil
}

// templateConfig applies database-specific templating to config
func (g *configGenerator) templateConfig(cfg *Config, dbName string) {
	// Replace ${NETWORK_NAME} placeholder in ClickHouse URL if present
	cfg.ClickHouse.URL = strings.ReplaceAll(cfg.ClickHouse.URL, "${NETWORK_NAME}", dbName)

	// Ensure Redis prefix is properly namespaced
	if !strings.Contains(cfg.Redis.Prefix, dbName) {
		g.log.WithField("database", dbName).Warn("Redis prefix does not contain database name")
	}
}

// buildTestOverrides creates test-optimized overrides by loading overrides.tests.yaml
func (g *configGenerator) buildTestOverrides(models []string) map[string]*ModelOverrides {
	// Load overrides from overrides.tests.yaml
	overridesPath := "overrides.tests.yaml"
	data, err := os.ReadFile(overridesPath)
	if err != nil {
		g.log.WithError(err).Warn("failed to read overrides.tests.yaml, using default test overrides")
		return g.buildDefaultTestOverrides(models)
	}

	// Parse the overrides file
	var overridesConfig struct {
		Models struct {
			Overrides map[string]*ModelOverrides `yaml:"overrides"`
		} `yaml:"models"`
	}

	if err := yaml.Unmarshal(data, &overridesConfig); err != nil {
		g.log.WithError(err).Warn("failed to parse overrides.tests.yaml, using default test overrides")
		return g.buildDefaultTestOverrides(models)
	}

	g.log.WithFields(logrus.Fields{
		"from_file": len(overridesConfig.Models.Overrides),
	}).Info("applying overrides.tests.yaml")

	// Use ALL overrides from overrides.tests.yaml
	// Don't filter by test model list - CBT discovers dependencies automatically
	// and those dependencies need their overrides applied too (e.g., lag=0 for external models)
	allOverrides := make(map[string]*ModelOverrides)
	for modelName, override := range overridesConfig.Models.Overrides {
		allOverrides[modelName] = override
	}

	// Add default fast schedules for transformation models not in overrides file
	for _, modelName := range models {
		if allOverrides[modelName] != nil {
			continue // Already have override
		}

		// Check if it's a transformation model
		transformPath := filepath.Join(g.transformationDir, modelName+".sql")
		if _, err := os.Stat(transformPath); err == nil {
			// Add default fast schedule override
			allOverrides[modelName] = &ModelOverrides{}
			allOverrides[modelName].Config.Schedules = map[string]string{
				"forwardfill": "@every 5s",
				"backfill":    "@every 5s",
			}
		}
	}

	return allOverrides
}

// buildDefaultTestOverrides creates fallback overrides if overrides.tests.yaml is not available
func (g *configGenerator) buildDefaultTestOverrides(models []string) map[string]*ModelOverrides {
	overrides := make(map[string]*ModelOverrides)

	// Set lag=0 for all external models, fast schedules for transformations
	for _, modelName := range models {
		// Check if it's an external model
		externalPath := filepath.Join(g.externalDir, modelName+".sql")
		if _, err := os.Stat(externalPath); err == nil {
			lag := 0
			overrides[modelName] = &ModelOverrides{}
			overrides[modelName].Config.Lag = &lag
			continue
		}

		// Check if it's a transformation model
		transformPath := filepath.Join(g.transformationDir, modelName+".sql")
		if _, err := os.Stat(transformPath); err == nil {
			overrides[modelName] = &ModelOverrides{}
			overrides[modelName].Config.Schedule = "@every 5s"
			continue
		}
	}

	g.log.WithField("count", len(overrides)).Debug("generated default test overrides")

	return overrides
}
