// Package testdef provides test definition loading and validation.
// Test definitions specify what tests to run (model specs, assertions, external data)
// as opposed to how to run them (see testing.TestDefinition for execution parameters).
package testdef

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
	errModelNameRequired                 = errors.New("model name is required")
	errNetworkRequired                   = errors.New("network is required")
	errSpecRequired                      = errors.New("spec is required")
	errExternalTableMissingURL           = errors.New("external table missing URL")
	errExternalTableMissingNetworkColumn = errors.New("external table missing network_column")
	errAssertionMissingName              = errors.New("assertion missing name")
	errAssertionMissingSQL               = errors.New("assertion missing SQL")
	errAssertionMissingExpected          = errors.New("assertion missing expected values")
)

// TestDefinition represents a complete per-model test specification.
// This defines what a test should do (model, data, assertions)
// rather than how it should execute (see testing.TestConfig).
type TestDefinition struct {
	Model        string                    `yaml:"model"`
	Network      string                    `yaml:"network"`
	Spec         string                    `yaml:"spec"`
	ExternalData map[string]*ExternalTable `yaml:"external_data"`
	Assertions   []*Assertion              `yaml:"assertions"`
}

// ExternalTable defines parquet data for an external table.
type ExternalTable struct {
	URL           string `yaml:"url"`
	NetworkColumn string `yaml:"network_column"`
}

// Assertion represents a single SQL test.
type Assertion struct {
	Name     string                 `yaml:"name"`
	SQL      string                 `yaml:"sql"`
	Expected map[string]interface{} `yaml:"expected"`
}

// Loader loads test definition files.
type Loader interface {
	LoadForModel(spec, network, modelName string) (*TestDefinition, error)
	LoadForSpec(spec, network string) (map[string]*TestDefinition, error)
	LoadForModels(spec, network string, modelNames []string) (map[string]*TestDefinition, error)
}

type loader struct {
	baseDir string
	log     logrus.FieldLogger
}

// NewLoader creates a new test definition loader.
func NewLoader(log logrus.FieldLogger, baseDir string) Loader {
	return &loader{
		baseDir: baseDir,
		log:     log.WithField("component", "testdef_loader"),
	}
}

// LoadForModel loads a single model's test definition.
func (l *loader) LoadForModel(spec, network, modelName string) (*TestDefinition, error) {
	path := l.buildPath(network, spec, modelName)

	l.log.WithFields(logrus.Fields{
		"spec":    spec,
		"network": network,
		"model":   modelName,
		"path":    path,
	}).Debug("loading test definition for model")

	config, err := l.loadFile(path)
	if err != nil {
		return nil, fmt.Errorf("loading config from %s: %w", path, err)
	}

	if err := l.validateDefinition(config); err != nil {
		return nil, fmt.Errorf("validating definition for %s: %w", modelName, err)
	}

	return config, nil
}

// LoadForSpec loads all model test definitions for a spec.
func (l *loader) LoadForSpec(spec, network string) (map[string]*TestDefinition, error) {
	dir := filepath.Join(l.baseDir, network, spec, "models")

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", dir, err)
	}

	configs := make(map[string]*TestDefinition)

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

		if err := l.validateDefinition(config); err != nil {
			l.log.WithError(err).WithField("model", modelName).Warn("invalid definition, skipping")
			continue
		}

		configs[modelName] = config
	}

	return configs, nil
}

// LoadForModels loads test definitions for specific models.
func (l *loader) LoadForModels(spec, network string, modelNames []string) (map[string]*TestDefinition, error) {
	l.log.WithFields(logrus.Fields{
		"spec":    spec,
		"network": network,
		"models":  modelNames,
	}).Debug("loading test definitions for specific models")

	configs := make(map[string]*TestDefinition, len(modelNames))

	for _, modelName := range modelNames {
		config, err := l.LoadForModel(spec, network, modelName)
		if err != nil {
			return nil, fmt.Errorf("loading config for %s: %w", modelName, err)
		}

		configs[modelName] = config
	}

	return configs, nil
}

// loadFile reads and parses a YAML test definition file
func (l *loader) loadFile(path string) (*TestDefinition, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: Reading test config from trusted paths
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var config TestDefinition
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing yaml: %w", err)
	}

	return &config, nil
}

// validateDefinition ensures the test definition is valid
func (l *loader) validateDefinition(definition *TestDefinition) error {
	if definition.Model == "" {
		return errModelNameRequired
	}

	if definition.Network == "" {
		return errNetworkRequired
	}

	if definition.Spec == "" {
		return errSpecRequired
	}

	// Validate external data if present
	if definition.ExternalData != nil {
		for tableName, extData := range definition.ExternalData {
			if extData.URL == "" {
				return fmt.Errorf("%w: %s", errExternalTableMissingURL, tableName)
			}

			if extData.NetworkColumn == "" {
				return fmt.Errorf("%w: %s", errExternalTableMissingNetworkColumn, tableName)
			}
		}
	}

	// Validate assertions
	if len(definition.Assertions) == 0 {
		l.log.WithField("model", definition.Model).Warn("no assertions defined for model")
	}

	for i, assertion := range definition.Assertions {
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

// buildPath constructs the file path for a model's test definition
func (l *loader) buildPath(network, spec, modelName string) string {
	return filepath.Join(l.baseDir, network, spec, "models", modelName+".yaml")
}
