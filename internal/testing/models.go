// Package testing provides end-to-end test orchestration and execution.
package testing

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/ethpandaops/xatu-cbt/internal/testing/testdef"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

// ModelType represents the category of model.
type ModelType string

const (
	// ModelTypeExternal represents external data source models
	ModelTypeExternal ModelType = "external"
	// ModelTypeTransformation represents transformation models
	ModelTypeTransformation ModelType = "transformation"
	// ModelTypeUnknown represents an unknown model type
	ModelTypeUnknown ModelType = "unknown"

	// DefaultNetworkColumn is the column name used to filter by network in external tables.
	DefaultNetworkColumn = "meta_network_name"
)

// ModelMetadata represents a parsed model with all cached metadata.
// Parsing happens once during initialization to eliminate redundant file reads.
type ModelMetadata struct {
	Name          string   // Table name (inferred from filename or frontmatter)
	ExecutionType string   // incremental, scheduled, or empty for external models
	Dependencies  []string // List of table dependencies
	SourceDB      string   // Source database for cross-database external models (empty = default)
	SourceTable   string   // Actual table name in source database (empty = same as Name)
}

// ExternalTableRef describes the source location of an external table in ClickHouse.
// For standard external models, SourceDB and SourceTable are empty (default database, model name as table).
// For cross-database models (e.g., observoor.cpu_utilization), these fields specify the actual source location.
type ExternalTableRef struct {
	ModelName   string // Model identifier used in the test database
	SourceDB    string // Source database ("" = default)
	SourceTable string // Actual source table name ("" = same as ModelName)
}

// Dependencies contains resolved dependency information for test execution.
type Dependencies struct {
	TargetModel          *ModelMetadata     // The model being tested
	TransformationModels []*ModelMetadata   // Transformations in execution order (topologically sorted)
	ExternalTables       []string           // Leaf external table names required (model identifiers)
	ExternalTableRefs    []ExternalTableRef // Source references for external tables (includes cross-database info)
	ParquetURLs          map[string]string  // External table name → parquet URL (from testConfig)
}

// ModelCache caches parsed model metadata and handles dependency resolution.
// All models are parsed once during LoadAll() and cached for the lifetime of the cache.
type ModelCache struct {
	externalModels       map[string]*ModelMetadata
	transformationModels map[string]*ModelMetadata
	log                  logrus.FieldLogger
	mu                   sync.RWMutex
}

// Frontmatter represents YAML frontmatter in SQL files.
type Frontmatter struct {
	Database     string        `yaml:"database"` // Source database for cross-database external models
	Table        string        `yaml:"table"`
	Type         string        `yaml:"type"`         // incremental or scheduled
	Dependencies []interface{} `yaml:"dependencies"` // Can be []string or [][]string (OR dependencies)
	Cache        *struct {
		IncrementalScanInterval string `yaml:"incremental_scan_interval"`
		FullScanInterval        string `yaml:"full_scan_interval"`
	} `yaml:"cache,omitempty"`
}

// NewModelCache creates a new model cache.
func NewModelCache(log logrus.FieldLogger) *ModelCache {
	return &ModelCache{
		externalModels:       make(map[string]*ModelMetadata),
		transformationModels: make(map[string]*ModelMetadata),
		log:                  log.WithField("component", "model_cache"),
	}
}

// LoadAll parses all models from external and transformation directories in parallel.
// Models are cached to eliminate redundant file reads throughout the test lifecycle.
func (c *ModelCache) LoadAll(ctx context.Context, externalDir, transformationDir string) error {
	c.log.Debug("loading all models")

	g, gctx := errgroup.WithContext(ctx)

	// Parse external models in parallel
	g.Go(func() error {
		models, err := c.parseDirectory(gctx, externalDir, ModelTypeExternal)
		if err != nil {
			return fmt.Errorf("parsing external models from %s: %w", externalDir, err)
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		for _, model := range models {
			c.externalModels[model.Name] = model
		}

		c.log.WithField("count", len(models)).Info("loaded external models")

		return nil
	})

	// Parse transformation models in parallel
	g.Go(func() error {
		models, err := c.parseDirectory(gctx, transformationDir, ModelTypeTransformation)
		if err != nil {
			return fmt.Errorf("parsing transformation models from %s: %w", transformationDir, err)
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		for _, model := range models {
			c.transformationModels[model.Name] = model
		}

		c.log.WithField("count", len(models)).Info("loaded transformation models")

		return nil
	})

	return g.Wait()
}

// ResolveTestDependencies performs dependency resolution and validation for a test.
// Returns the complete dependency graph with transformations topologically sorted.
func (c *ModelCache) ResolveTestDependencies(testConfig *testdef.TestDefinition) (*Dependencies, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Find the target model
	targetModel, isTransformation := c.transformationModels[testConfig.Model]
	if !isTransformation {
		// Check if it's an external model
		externalModel, isExternal := c.externalModels[testConfig.Model]
		if !isExternal {
			return nil, fmt.Errorf("model %s not found in external or transformation models", testConfig.Model) //nolint:err113 // Include model name for debugging
		}

		// External model - simple case
		deps := &Dependencies{
			TargetModel:          externalModel,
			TransformationModels: []*ModelMetadata{},
			ExternalTables:       []string{externalModel.Name},
			ExternalTableRefs: []ExternalTableRef{{
				ModelName:   externalModel.Name,
				SourceDB:    externalModel.SourceDB,
				SourceTable: externalModel.SourceTable,
			}},
			ParquetURLs: make(map[string]string),
		}

		// Build parquet URL map
		if testConfig.ExternalData != nil {
			for tableName, extData := range testConfig.ExternalData {
				deps.ParquetURLs[tableName] = extData.URL
			}
		}

		// Validate that required external data is provided
		if err := c.validateExternalData(deps.ExternalTables, testConfig.ExternalData); err != nil {
			return nil, err
		}

		return deps, nil
	}

	// Transformation model - build dependency graph
	transformations, err := c.buildDependencyGraph(testConfig.Model, make(map[string]bool))
	if err != nil {
		return nil, fmt.Errorf("building dependency graph: %w", err)
	}

	// Detect circular dependencies
	if circErr := c.detectCircularDependencies(transformations); circErr != nil {
		return nil, circErr
	}

	// Extract leaf external tables and build dependency map for error messages
	externalTables, externalDependents := c.extractLeafExternalTablesWithDependents(transformations)

	// Warn about any missing external tables (non-fatal - assertions will catch actual problems)
	c.warnMissingExternalData(externalTables, externalDependents, testConfig.ExternalData)

	// Build parquet URL map from explicitly provided external data
	parquetURLs := make(map[string]string)
	for _, tableName := range externalTables {
		if extData, ok := testConfig.ExternalData[tableName]; ok {
			parquetURLs[tableName] = extData.URL
		}
	}

	// Topologically sort transformations
	sortedTransformations, err := c.topologicalSort(transformations)
	if err != nil {
		return nil, fmt.Errorf("topological sort: %w", err)
	}

	// Build ExternalTableRefs with cross-database source info
	extRefs := make([]ExternalTableRef, 0, len(externalTables))
	for _, tableName := range externalTables {
		ref := ExternalTableRef{ModelName: tableName}
		if ext, ok := c.externalModels[tableName]; ok {
			ref.SourceDB = ext.SourceDB
			ref.SourceTable = ext.SourceTable
		}

		extRefs = append(extRefs, ref)
	}

	deps := &Dependencies{
		TargetModel:          targetModel,
		TransformationModels: sortedTransformations,
		ExternalTables:       externalTables,
		ExternalTableRefs:    extRefs,
		ParquetURLs:          parquetURLs,
	}

	// Extract transformation names for logging
	transformationNames := make([]string, len(sortedTransformations))
	for i, t := range sortedTransformations {
		transformationNames[i] = t.Name
	}

	c.log.WithFields(logrus.Fields{
		"model":                testConfig.Model,
		"transformations":      len(deps.TransformationModels),
		"transformation_names": transformationNames,
		"external_tables":      len(deps.ExternalTables),
	}).Debug("resolved dependencies")

	return deps, nil
}

// IsExternalModel checks if a model exists in the external models.
func (c *ModelCache) IsExternalModel(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.externalModels[name]

	return exists
}

// GetExternalModel returns the metadata for an external model, or nil if not found.
func (c *ModelCache) GetExternalModel(name string) *ModelMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.externalModels[name]
}

// IsTransformationModel checks if a model exists in the transformations.
func (c *ModelCache) IsTransformationModel(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.transformationModels[name]

	return exists
}

// ListAllModels returns a sorted list of all model names (both external and transformation).
func (c *ModelCache) ListAllModels() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	models := make([]string, 0, len(c.externalModels)+len(c.transformationModels))

	for name := range c.externalModels {
		models = append(models, name)
	}

	for name := range c.transformationModels {
		models = append(models, name)
	}

	return models
}

// parseDirectory parses all model files in a directory.
// Caller must not hold c.mu lock as this method doesn't acquire it.
func (c *ModelCache) parseDirectory(_ context.Context, dir string, modelType ModelType) ([]*ModelMetadata, error) {
	c.log.WithFields(logrus.Fields{
		"dir":  dir,
		"type": modelType,
	}).Debug("parsing models in directory")

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory: %w", err)
	}

	models := make([]*ModelMetadata, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only process .sql and .yml files
		ext := filepath.Ext(entry.Name())
		if ext != ".sql" && ext != ".yml" && ext != ".yaml" {
			continue
		}

		model, err := c.parseModel(filepath.Join(dir, entry.Name()), modelType)
		if err != nil {
			c.log.WithError(err).WithField("file", entry.Name()).Warn("failed to parse model, skipping")
			continue
		}

		models = append(models, model)
	}

	c.log.WithFields(logrus.Fields{
		"dir":   dir,
		"count": len(models),
	}).Debug("parsed models")

	return models, nil
}

// parseModel parses a single model file and extracts metadata.
// Caller must not hold c.mu lock as this method doesn't acquire it.
func (c *ModelCache) parseModel(path string, _ ModelType) (*ModelMetadata, error) {
	//nolint:gosec // G304: Reading model files from trusted paths
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var frontmatter *Frontmatter

	// Check if this is a pure YAML file (.yml or .yaml) - parse directly
	// Otherwise, extract frontmatter from SQL files (content between --- delimiters)
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".yml" || ext == ".yaml" {
		// Pure YAML file - parse directly
		frontmatter = &Frontmatter{}
		if unmarshalErr := yaml.Unmarshal(content, frontmatter); unmarshalErr != nil {
			return nil, fmt.Errorf("parsing yaml file %s: %w", path, unmarshalErr)
		}
	} else {
		// SQL file with frontmatter - extract frontmatter section
		var extractErr error
		frontmatter, _, extractErr = c.extractFrontmatter(string(content))
		if extractErr != nil {
			frontmatter = &Frontmatter{}
		}
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
			normalized := c.normalizeDependency(v)
			dependencies = append(dependencies, normalized)
		case []interface{}:
			// OR dependency: ["{{external}}.table1", "{{external}}.table2"]
			// Flatten - include all options since CBT needs to know about all of them
			for _, orDep := range v {
				if depStr, ok := orDep.(string); ok {
					normalized := c.normalizeDependency(depStr)
					dependencies = append(dependencies, normalized)
				}
			}
		}
	}

	// Normalize execution type (incremental, scheduled, or empty)
	executionType := strings.ToLower(strings.TrimSpace(frontmatter.Type))
	if executionType != "incremental" && executionType != "scheduled" {
		executionType = "" // Default to empty for external models or unknown types
	}

	// Capture cross-database source info for external models.
	// When database is set in frontmatter (e.g., "observoor"), the actual source table
	// lives in a different database (e.g., observoor.cpu_utilization) than the model name implies.
	var sourceDB, sourceTable string
	if frontmatter.Database != "" {
		sourceDB = frontmatter.Database
		sourceTable = frontmatter.Table
	}

	return &ModelMetadata{
		Name:          tableName,
		ExecutionType: executionType,
		Dependencies:  dependencies,
		SourceDB:      sourceDB,
		SourceTable:   sourceTable,
	}, nil
}

// extractFrontmatter extracts YAML frontmatter from SQL content
func (c *ModelCache) extractFrontmatter(content string) (*Frontmatter, string, error) {
	// Match YAML frontmatter between --- delimiters
	var (
		re      = regexp.MustCompile(`(?s)^---\s*\n(.*?)\n---\s*\n(.*)`)
		matches = re.FindStringSubmatch(content)
	)

	if len(matches) < 3 {
		return nil, "", fmt.Errorf("no frontmatter found") //nolint:err113 // Standard parsing error
	}

	var (
		frontmatterYAML = matches[1]
		sql             = matches[2]
	)

	var frontmatter Frontmatter
	if err := yaml.Unmarshal([]byte(frontmatterYAML), &frontmatter); err != nil {
		return nil, "", fmt.Errorf("parsing frontmatter yaml: %w", err)
	}

	return &frontmatter, sql, nil
}

// normalizeDependency converts dependency references to table names
// Handles: {{external}}.table_name → table_name
//
//	{{transformation}}.table_name → table_name
func (c *ModelCache) normalizeDependency(dep string) string {
	// Extract table name from {{external}}.table_name or {{transformation}}.table_name
	var (
		re      = regexp.MustCompile(`\{\{(?:external|transformation)\}\}\.(.+)`)
		matches = re.FindStringSubmatch(dep)
	)

	if len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}

	// If no template pattern found, return as-is (already normalised).
	return strings.TrimSpace(dep)
}

// buildDependencyGraph recursively builds the dependency graph for a model.
// Caller must hold c.mu read lock.
func (c *ModelCache) buildDependencyGraph(modelName string, visited map[string]bool) ([]*ModelMetadata, error) {
	if visited[modelName] {
		// Already visited, skip to avoid infinite recursion
		return nil, nil
	}
	visited[modelName] = true

	// Check if it's a transformation
	model, isTransformation := c.transformationModels[modelName]
	if !isTransformation {
		// It's an external model, no dependencies to recurse
		return nil, nil
	}

	models := []*ModelMetadata{model}

	// Recursively resolve dependencies
	for _, dep := range model.Dependencies {
		depModels, err := c.buildDependencyGraph(dep, visited)
		if err != nil {
			return nil, err
		}
		models = append(models, depModels...)
	}

	return models, nil
}

// extractLeafExternalTables finds all external table dependencies.
// Caller must hold c.mu read lock.
func (c *ModelCache) extractLeafExternalTables(transformations []*ModelMetadata) []string { //nolint:unused // kept for future use
	externalSet := make(map[string]bool)

	for _, model := range transformations {
		for _, dep := range model.Dependencies {
			// Check if dependency is an external model
			if _, isExternal := c.externalModels[dep]; isExternal {
				externalSet[dep] = true
			}
		}
	}

	// Convert set to slice
	externals := make([]string, 0, len(externalSet))
	for ext := range externalSet {
		externals = append(externals, ext)
	}

	return externals
}

// extractLeafExternalTablesWithDependents finds all external table dependencies
// and returns a map of which transformation(s) depend on each external table.
// Caller must hold c.mu read lock.
func (c *ModelCache) extractLeafExternalTablesWithDependents(transformations []*ModelMetadata) (externals []string, dependents map[string][]string) {
	externalSet := make(map[string]bool)
	dependents = make(map[string][]string) // external table -> list of transformations that depend on it

	for _, model := range transformations {
		for _, dep := range model.Dependencies {
			// Check if dependency is an external model
			if _, isExternal := c.externalModels[dep]; isExternal {
				externalSet[dep] = true
				dependents[dep] = append(dependents[dep], model.Name)
			}
		}
	}

	// Convert set to slice
	externals = make([]string, 0, len(externalSet))
	for ext := range externalSet {
		externals = append(externals, ext)
	}

	return externals, dependents
}

// warnMissingExternalData logs warnings for any external tables that are required
// but not provided in the test config. This is non-fatal - the test will proceed
// and assertions will catch any actual problems from missing data.
func (c *ModelCache) warnMissingExternalData(
	required []string,
	dependents map[string][]string,
	provided map[string]*testdef.ExternalTable,
) {
	var missingDetails []string

	for _, tableName := range required {
		if provided == nil || provided[tableName] == nil {
			// Build helpful message showing which transformation(s) need this table
			deps := dependents[tableName]
			if len(deps) > 0 {
				missingDetails = append(missingDetails, fmt.Sprintf("  - %s (dependency of: %s)", tableName, strings.Join(deps, ", ")))
			} else {
				missingDetails = append(missingDetails, fmt.Sprintf("  - %s", tableName))
			}
		}
	}

	if len(missingDetails) > 0 {
		c.log.Warnf("test config missing external_data (tables will be empty):\n%s", strings.Join(missingDetails, "\n"))
	}
}

// topologicalSort sorts transformations in execution order (dependencies first).
// Caller must hold c.mu read lock.
func (c *ModelCache) topologicalSort(transformations []*ModelMetadata) ([]*ModelMetadata, error) {
	// Build adjacency list and in-degree map
	adjList := make(map[string][]*ModelMetadata)
	inDegree := make(map[string]int)
	modelMap := make(map[string]*ModelMetadata)

	for _, model := range transformations {
		modelMap[model.Name] = model
		if _, ok := inDegree[model.Name]; !ok {
			inDegree[model.Name] = 0
		}

		for _, dep := range model.Dependencies {
			// Only consider transformation dependencies for sorting
			if _, isTransformation := c.transformationModels[dep]; isTransformation {
				adjList[dep] = append(adjList[dep], model)
				inDegree[model.Name]++
			}
		}
	}

	// Kahn's algorithm for topological sort
	queue := make([]*ModelMetadata, 0)
	for name, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, modelMap[name])
		}
	}

	sorted := make([]*ModelMetadata, 0, len(transformations))
	for len(queue) > 0 {
		// Dequeue
		current := queue[0]
		queue = queue[1:]
		sorted = append(sorted, current)

		// Reduce in-degree of neighbors
		for _, neighbor := range adjList[current.Name] {
			inDegree[neighbor.Name]--
			if inDegree[neighbor.Name] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(sorted) != len(transformations) {
		return nil, fmt.Errorf("circular dependency detected in transformations") //nolint:err113 // Standard validation error
	}

	return sorted, nil
}

// validateExternalData checks that all required external tables are provided.
func (c *ModelCache) validateExternalData(required []string, provided map[string]*testdef.ExternalTable) error {
	missing := make([]string, 0)

	for _, tableName := range required {
		if provided == nil {
			missing = append(missing, tableName)
			continue
		}

		if _, ok := provided[tableName]; !ok {
			missing = append(missing, tableName)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("test config missing required external_data tables: %v", missing) //nolint:err113 // Include missing tables for debugging
	}

	return nil
}

// detectCircularDependencies uses DFS to detect cycles.
// Caller must hold c.mu read lock.
func (c *ModelCache) detectCircularDependencies(transformations []*ModelMetadata) error {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for _, model := range transformations {
		if !visited[model.Name] {
			if c.hasCycleDFS(model.Name, visited, recStack) {
				return fmt.Errorf("circular dependency detected involving model %s", model.Name) //nolint:err113 // Include model name for debugging
			}
		}
	}

	return nil
}

// hasCycleDFS performs DFS to detect cycles.
// Caller must hold c.mu read lock.
func (c *ModelCache) hasCycleDFS(modelName string, visited, recStack map[string]bool) bool {
	visited[modelName] = true
	recStack[modelName] = true

	model, isTransformation := c.transformationModels[modelName]
	if !isTransformation {
		recStack[modelName] = false
		return false
	}

	for _, dep := range model.Dependencies {
		if !visited[dep] {
			if c.hasCycleDFS(dep, visited, recStack) {
				return true
			}
		} else if recStack[dep] {
			return true
		}
	}

	recStack[modelName] = false

	return false
}
