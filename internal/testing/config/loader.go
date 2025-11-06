// Package config provides test configuration loading and validation.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var (
	errModelNameRequired      = errors.New("model name is required")
	errNetworkRequired        = errors.New("network is required")
	errSpecRequired           = errors.New("spec is required")
	errExternalTableMissingURL = errors.New("external table missing URL")
	errExternalTableMissingNetworkColumn = errors.New("external table missing network_column")
	errAssertionMissingName   = errors.New("assertion missing name")
	errAssertionMissingSQL    = errors.New("assertion missing SQL")
	errAssertionMissingExpected = errors.New("assertion missing expected values")
)

// TestConfig represents a complete per-model test configuration
type TestConfig struct {
	Model        string                    `yaml:"model"`
	Network      string                    `yaml:"network"`
	Spec         string                    `yaml:"spec"`
	ExternalData map[string]*ExternalTable `yaml:"external_data"`
	Assertions   []*Assertion              `yaml:"assertions"`
}

// ExternalTable defines parquet data for an external table
type ExternalTable struct {
	URL           string `yaml:"url"`
	NetworkColumn string `yaml:"network_column"`
}

// Assertion represents a single SQL test
type Assertion struct {
	Name     string                 `yaml:"name"`
	SQL      string                 `yaml:"sql"`
	Expected map[string]interface{} `yaml:"expected"`
}

// Loader loads test configuration files
type Loader interface {
	LoadForModel(spec, network, modelName string) (*TestConfig, error)
	LoadForSpec(spec, network string) (map[string]*TestConfig, error)
	LoadForModels(spec, network string, modelNames []string) (map[string]*TestConfig, error)
}

type loader struct {
	baseDir string
	log     logrus.FieldLogger
}

// NewLoader creates a new test config loader
func NewLoader(log logrus.FieldLogger, baseDir string) Loader {
	return &loader{
		baseDir: baseDir,
		log:     log.WithField("component", "config_loader"),
	}
}

// LoadForModel loads a single model's test configuration
func (l *loader) LoadForModel(spec, network, modelName string) (*TestConfig, error) {
	path := l.buildPath(network, spec, modelName)

	l.log.WithFields(logrus.Fields{
		"spec":    spec,
		"network": network,
		"model":   modelName,
		"path":    path,
	}).Debug("loading test config for model")

	config, err := l.loadFile(path)
	if err != nil {
		return nil, fmt.Errorf("loading config from %s: %w", path, err)
	}

	if err := l.validateConfig(config); err != nil {
		return nil, fmt.Errorf("validating config for %s: %w", modelName, err)
	}

	return config, nil
}

// LoadForSpec loads all model test configurations for a spec
func (l *loader) LoadForSpec(spec, network string) (map[string]*TestConfig, error) {
	dir := filepath.Join(l.baseDir, network, spec, "models")

	l.log.WithFields(logrus.Fields{
		"spec":    spec,
		"network": network,
		"dir":     dir,
	}).Debug("loading all test configs for spec")

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", dir, err)
	}

	configs := make(map[string]*TestConfig)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		modelName := strings.TrimSuffix(entry.Name(), ".yaml")
		path := filepath.Join(dir, entry.Name())

		config, err := l.loadFile(path)
		if err != nil {
			l.log.WithError(err).WithField("file", entry.Name()).Warn("failed to load config, skipping")
			continue
		}

		if err := l.validateConfig(config); err != nil {
			l.log.WithError(err).WithField("model", modelName).Warn("invalid config, skipping")
			continue
		}

		configs[modelName] = config
	}

	l.log.WithField("count", len(configs)).Debug("loaded test configs")

	return configs, nil
}

// LoadForModels loads test configurations for specific models
func (l *loader) LoadForModels(spec, network string, modelNames []string) (map[string]*TestConfig, error) {
	l.log.WithFields(logrus.Fields{
		"spec":    spec,
		"network": network,
		"models":  modelNames,
	}).Debug("loading test configs for specific models")

	configs := make(map[string]*TestConfig, len(modelNames))

	for _, modelName := range modelNames {
		config, err := l.LoadForModel(spec, network, modelName)
		if err != nil {
			return nil, fmt.Errorf("loading config for %s: %w", modelName, err)
		}

		configs[modelName] = config
	}

	return configs, nil
}

// loadFile reads and parses a YAML test config file
func (l *loader) loadFile(path string) (*TestConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var config TestConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing yaml: %w", err)
	}

	return &config, nil
}

// validateConfig ensures the test config is valid
func (l *loader) validateConfig(config *TestConfig) error {
	if config.Model == "" {
		return errModelNameRequired
	}

	if config.Network == "" {
		return errNetworkRequired
	}

	if config.Spec == "" {
		return errSpecRequired
	}

	// Validate external data if present
	if config.ExternalData != nil {
		for tableName, extData := range config.ExternalData {
			if extData.URL == "" {
				return fmt.Errorf("%w: %s", errExternalTableMissingURL, tableName)
			}

			if extData.NetworkColumn == "" {
				return fmt.Errorf("%w: %s", errExternalTableMissingNetworkColumn, tableName)
			}
		}
	}

	// Validate assertions
	if len(config.Assertions) == 0 {
		l.log.WithField("model", config.Model).Warn("no assertions defined for model")
	}

	for i, assertion := range config.Assertions {
		if assertion.Name == "" {
			return fmt.Errorf("%w at index %d", errAssertionMissingName, i)
		}

		if assertion.SQL == "" {
			return fmt.Errorf("%w: %s", errAssertionMissingSQL, assertion.Name)
		}

		if len(assertion.Expected) == 0 {
			return fmt.Errorf("%w: %s", errAssertionMissingExpected, assertion.Name)
		}
	}

	return nil
}

// buildPath constructs the file path for a model's test config
func (l *loader) buildPath(network, spec, modelName string) string {
	return filepath.Join(l.baseDir, network, spec, "models", modelName+".yaml")
}
