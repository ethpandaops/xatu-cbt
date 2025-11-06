// Package cbt provides CBT configuration generation and transformation execution.
package cbt

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// ConfigGenerator generates CBT config files
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

// ModelOverrides contains per-model configuration overrides
type ModelOverrides struct {
	Config struct {
		Lag       *int              `yaml:"lag,omitempty"`
		Schedule  string            `yaml:"schedule,omitempty"`  // For scheduled models
		Schedules map[string]string `yaml:"schedules,omitempty"` // For incremental models (forwardfill/backfill)
	} `yaml:"config"`
}

// CBTConfig represents the structure of config.yml
type CBTConfig struct {
	ClickHouse struct {
		URL         string `yaml:"url"`
		Cluster     string `yaml:"cluster,omitempty"`     // Cluster name for ON CLUSTER queries
		LocalSuffix string `yaml:"localSuffix,omitempty"` // Suffix for local tables (e.g., "_local")
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
		Prefix string `yaml:"prefix"` // ANTI-FLAKE #5: Namespace keys per test
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
		Env       map[string]string          `yaml:"env,omitempty"`       // Global environment variables for templates
		Overrides map[string]*ModelOverrides `yaml:"overrides,omitempty"` // Model-specific overrides for testing
	} `yaml:"models"`
	Scheduler struct {
		Concurrency int `yaml:"concurrency"`
	} `yaml:"scheduler"`
	Worker struct {
		Concurrency int `yaml:"concurrency"`
	} `yaml:"worker"`
}

// NewConfigGenerator creates a new CBT config generator
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

// GenerateForModels generates a CBT config for specific models
func (g *configGenerator) GenerateForModels(network, dbName string, models []string, outputPath string) error {
	g.log.WithFields(logrus.Fields{
		"network":  network,
		"database": dbName,
		"models":   models,
		"output":   outputPath,
	}).Debug("generating CBT config")

	// Build model paths separated by type
	externalPaths, transformationPaths, err := g.buildModelPaths(models)
	if err != nil {
		return fmt.Errorf("building model paths: %w", err)
	}

	// Create config structure
	config := &CBTConfig{}

	// Configure ClickHouse connection
	// CBT uses HTTP interface, convert clickhouse:// URL to http://
	clickhouseHTTPURL := strings.Replace(g.clickhouseURL, "clickhouse://", "http://", 1)
	// Replace localhost with container hostname and adjust ports for Docker network access
	// Host ports (9000 native, 8123 HTTP) are mapped differently than container internal ports
	clickhouseHTTPURL = strings.Replace(clickhouseHTTPURL, "localhost:9000", "xatu-cbt-clickhouse-01:8123", 1)
	clickhouseHTTPURL = strings.Replace(clickhouseHTTPURL, "localhost:8123", "xatu-cbt-clickhouse-01:8123", 1)
	config.ClickHouse.URL = clickhouseHTTPURL

	// Configure cluster settings for transformation models in CBT cluster
	// Test databases are created ON CLUSTER 'cluster_2S_1R' (CBT cluster macros)
	// This enables DELETE to work on ReplicatedReplacingMergeTree tables using _local suffix
	// External models override this with their own defaultCluster: "xatu_cluster"
	config.ClickHouse.Cluster = "cluster_2S_1R"
	config.ClickHouse.LocalSuffix = "_local"

	// Configure admin tables - these are created by migrations in the test database
	config.ClickHouse.Admin.Incremental.Database = dbName
	config.ClickHouse.Admin.Incremental.Table = "admin_cbt_incremental"
	config.ClickHouse.Admin.Scheduled.Database = dbName
	config.ClickHouse.Admin.Scheduled.Table = "admin_cbt_scheduled"

	// Configure Redis with namespace per test database
	// ANTI-FLAKE #5: This prevents state leakage between concurrent tests
	// Replace localhost with container hostname and adjust port for Docker network access
	// Host port 6380 maps to container internal port 6379
	redisURL := strings.Replace(g.redisURL, "localhost:6380", "xatu-cbt-redis:6379", 1)
	redisURL = strings.Replace(redisURL, "localhost:6379", "xatu-cbt-redis:6379", 1)
	config.Redis.URL = redisURL
	config.Redis.Prefix = fmt.Sprintf("test:%s:", dbName)

	// Set model paths and default databases
	config.Models.External.Paths = externalPaths
	config.Models.External.DefaultCluster = "xatu_cluster" // External data is on remote xatu cluster
	config.Models.External.DefaultDatabase = "default"     // External data is in default database
	config.Models.Transformations.Paths = transformationPaths
	config.Models.Transformations.DefaultDatabase = dbName // Transformations use test database

	// Set global environment variables for template rendering
	// NETWORK is used in external model queries: WHERE meta_network_name = '{{ .env.NETWORK }}'
	config.Models.Env = map[string]string{
		"NETWORK": network,
	}

	// Configure for fast test execution
	config.Scheduler.Concurrency = 10
	config.Worker.Concurrency = 10

	// Add test-optimized overrides for all models
	config.Models.Overrides = g.buildTestOverrides(models)

	// Template config with database name
	g.templateConfig(config, dbName)

	// Write config to file
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(outputPath, data, 0644); err != nil { //nolint:gosec // G306: Config file with standard permissions
		return fmt.Errorf("writing config file: %w", err)
	}

	g.log.WithFields(logrus.Fields{
		"database":        dbName,
		"external":        len(externalPaths),
		"transformations": len(transformationPaths),
		"output":          outputPath,
	}).Debug("CBT config generated")

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
func (g *configGenerator) templateConfig(config *CBTConfig, dbName string) {
	// Replace ${NETWORK_NAME} placeholder in ClickHouse URL if present
	config.ClickHouse.URL = strings.ReplaceAll(config.ClickHouse.URL, "${NETWORK_NAME}", dbName)

	// Ensure Redis prefix is properly namespaced
	if !strings.Contains(config.Redis.Prefix, dbName) {
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
