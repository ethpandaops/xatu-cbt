// Package dependency provides model dependency resolution and topological sorting.
package dependency

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// ModelType represents the category of model
type ModelType string

const (
	// ModelTypeExternal represents external data source models
	ModelTypeExternal       ModelType = "external"
	// ModelTypeTransformation represents transformation models
	ModelTypeTransformation ModelType = "transformation"
)

// Model represents a CBT model with metadata
type Model struct {
	Name         string
	Type         ModelType
	Path         string
	Dependencies []string // Model names this depends on
	SQL          string
	Table        string // Table name from frontmatter or filename
}

// Frontmatter represents YAML frontmatter in SQL files
type Frontmatter struct {
	Table        string        `yaml:"table"`
	Dependencies []interface{} `yaml:"dependencies"` // Can be []string or [][]string (OR dependencies)
	Cache        *struct {
		IncrementalScanInterval string `yaml:"incremental_scan_interval"`
		FullScanInterval        string `yaml:"full_scan_interval"`
	} `yaml:"cache,omitempty"`
}

// Parser parses model files and extracts metadata
type Parser interface {
	ParseModel(path string, modelType ModelType) (*Model, error)
	ParseModelsInDir(dir string, modelType ModelType) ([]*Model, error)
}

type parser struct {
	log logrus.FieldLogger
}

// NewParser creates a new model parser
func NewParser(log logrus.FieldLogger) Parser {
	return &parser{
		log: log.WithField("component", "model_parser"),
	}
}

// ParseModel parses a single model file and extracts metadata
func (p *parser) ParseModel(path string, modelType ModelType) (*Model, error) {
	p.log.WithFields(logrus.Fields{
		"path": path,
		"type": modelType,
	}).Debug("parsing model file")

	// Read file content
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	// Extract frontmatter
	frontmatter, sql, err := p.extractFrontmatter(string(content))
	if err != nil {
		// If no frontmatter, create empty frontmatter
		frontmatter = &Frontmatter{}
		sql = string(content)
	}

	// Determine table name
	tableName := frontmatter.Table
	if tableName == "" {
		// Infer from filename
		filename := filepath.Base(path)
		tableName = strings.TrimSuffix(filename, filepath.Ext(filename))
	}

	// Normalize dependencies - handle both simple strings and OR dependencies (nested lists)
	dependencies := make([]string, 0)
	for _, dep := range frontmatter.Dependencies {
		switch v := dep.(type) {
		case string:
			// Simple dependency: "{{external}}.table_name"
			normalized := p.normalizeDependency(v)
			dependencies = append(dependencies, normalized)
		case []interface{}:
			// OR dependency: ["{{external}}.table1", "{{external}}.table2"]
			// Flatten - include all options since CBT needs to know about all of them
			for _, orDep := range v {
				if depStr, ok := orDep.(string); ok {
					normalized := p.normalizeDependency(depStr)
					dependencies = append(dependencies, normalized)
				}
			}
		}
	}

	model := &Model{
		Name:         tableName,
		Type:         modelType,
		Path:         path,
		Dependencies: dependencies,
		SQL:          sql,
		Table:        tableName,
	}

	p.log.WithFields(logrus.Fields{
		"name":         model.Name,
		"type":         model.Type,
		"dependencies": model.Dependencies,
	}).Debug("parsed model")

	return model, nil
}

// ParseModelsInDir parses all model files in a directory
func (p *parser) ParseModelsInDir(dir string, modelType ModelType) ([]*Model, error) {
	p.log.WithFields(logrus.Fields{
		"dir":  dir,
		"type": modelType,
	}).Debug("parsing models in directory")

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory: %w", err)
	}

	models := make([]*Model, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only process .sql and .yml files
		ext := filepath.Ext(entry.Name())
		if ext != ".sql" && ext != ".yml" && ext != ".yaml" {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		model, err := p.ParseModel(path, modelType)
		if err != nil {
			p.log.WithError(err).WithField("file", entry.Name()).Warn("failed to parse model, skipping")
			continue
		}

		models = append(models, model)
	}

	p.log.WithField("count", len(models)).Debug("parsed models")

	return models, nil
}

// extractFrontmatter extracts YAML frontmatter from SQL content
func (p *parser) extractFrontmatter(content string) (*Frontmatter, string, error) {
	// Match YAML frontmatter between --- delimiters
	re := regexp.MustCompile(`(?s)^---\s*\n(.*?)\n---\s*\n(.*)`)
	matches := re.FindStringSubmatch(content)

	if len(matches) < 3 {
		return nil, "", fmt.Errorf("no frontmatter found") //nolint:err113 // Standard parsing error
	}

	frontmatterYAML := matches[1]
	sql := matches[2]

	var frontmatter Frontmatter
	if err := yaml.Unmarshal([]byte(frontmatterYAML), &frontmatter); err != nil {
		return nil, "", fmt.Errorf("parsing frontmatter yaml: %w", err)
	}

	return &frontmatter, sql, nil
}

// normalizeDependency converts dependency references to table names
// Handles: {{external}}.table_name → table_name
//          {{transformation}}.table_name → table_name
func (p *parser) normalizeDependency(dep string) string {
	// Extract table name from {{external}}.table_name or {{transformation}}.table_name
	re := regexp.MustCompile(`\{\{(?:external|transformation)\}\}\.(.+)`)
	matches := re.FindStringSubmatch(dep)

	if len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}

	// If no template pattern found, return as-is (already normalized)
	return strings.TrimSpace(dep)
}
