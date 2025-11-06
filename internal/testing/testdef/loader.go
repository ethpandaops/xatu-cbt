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
	errAssertionMissingChecks            = errors.New("assertion must have either 'expected' values or 'assertions' checks")
	errTypedCheckMissingType             = errors.New("typed check missing type")
	errTypedCheckInvalidType             = errors.New("typed check has invalid type")
	errTypedCheckMissingColumn           = errors.New("typed check missing column")
	errTypedCheckMissingValue            = errors.New("typed check missing value")
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
// Supports two formats:
// 1. Exact match: Uses Expected map for exact value comparison
// 2. Typed checks: Uses Assertions list for comparison operators (>, <, >=, <=, ==, !=)
type Assertion struct {
	Name       string                 `yaml:"name"`
	SQL        string                 `yaml:"sql"`
	Expected   map[string]interface{} `yaml:"expected,omitempty"`   // Format 1: Exact match
	Assertions []*TypedCheck          `yaml:"assertions,omitempty"` // Format 2: Typed checks
}

// TypedCheck represents a typed assertion check with a comparison operator.
type TypedCheck struct {
	Type   string      `yaml:"type"`   // Comparison type: equals, greater_than, less_than, etc.
	Column string      `yaml:"column"` // Column name from SQL result
	Value  interface{} `yaml:"value"`  // Expected value to compare against
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

// validateDefinition ensures the test definition is valid.
//
//nolint:gocyclo // conditionals in loop, fine.
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

		// Must have either Expected (exact match) OR Assertions (typed checks)
		hasExpected := len(assertion.Expected) > 0
		hasTypedChecks := len(assertion.Assertions) > 0

		if !hasExpected && !hasTypedChecks {
			return fmt.Errorf("%w: %s", errAssertionMissingChecks, assertion.Name)
		}

		// Validate typed checks if present
		if hasTypedChecks {
			if err := l.validateTypedChecks(assertion.Name, assertion.Assertions); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateTypedChecks validates typed assertion checks
func (l *loader) validateTypedChecks(assertionName string, checks []*TypedCheck) error {
	validTypes := map[string]bool{
		"equals":                true,
		"equal":                 true,
		"not_equals":            true,
		"not_equal":             true,
		"greater_than":          true,
		"gt":                    true,
		"greater_than_or_equal": true,
		"gte":                   true,
		"less_than":             true,
		"lt":                    true,
		"less_than_or_equal":    true,
		"lte":                   true,
	}

	for i, check := range checks {
		if check.Type == "" {
			return fmt.Errorf("%w: assertion %s, check %d", errTypedCheckMissingType, assertionName, i)
		}

		if !validTypes[check.Type] {
			return fmt.Errorf("%w: assertion %s, check %d has type '%s' (must be one of: equals, not_equals, greater_than, greater_than_or_equal, less_than, less_than_or_equal)",
				errTypedCheckInvalidType, assertionName, i, check.Type)
		}

		if check.Column == "" {
			return fmt.Errorf("%w: assertion %s, check %d", errTypedCheckMissingColumn, assertionName, i)
		}

		if check.Value == nil {
			return fmt.Errorf("%w: assertion %s, check %d", errTypedCheckMissingValue, assertionName, i)
		}
	}

	return nil
}

// buildPath constructs the file path for a model's test definition
func (l *loader) buildPath(network, spec, modelName string) string {
	return filepath.Join(l.baseDir, network, spec, "models", modelName+".yaml")
}
