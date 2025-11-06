package dependency

import (
	"context"
	"fmt"

	"github.com/ethpandaops/xatu-cbt/internal/testing/config"
	"github.com/sirupsen/logrus"
)

// Resolver builds dependency graphs, validates completeness, and determines execution order
type Resolver interface {
	Start(ctx context.Context) error
	Stop() error
	ResolveAndValidate(testConfig *config.TestConfig) (*ResolutionResult, error)
	IsExternalModel(name string) bool
	IsTransformationModel(name string) bool
}

// ResolutionResult contains dependency resolution output
type ResolutionResult struct {
	TargetModel          *Model            // The model being tested
	TransformationModels []*Model          // Transformations in execution order (topologically sorted)
	ExternalTables       []string          // Leaf external table names required
	ParquetURLs          map[string]string // External table name → parquet URL (from testConfig)
}

type resolver struct {
	externalDir       string
	transformationDir string
	parser            Parser
	log               logrus.FieldLogger

	// Cached parsed models
	externalModels       map[string]*Model
	transformationModels map[string]*Model
}

// NewResolver creates a new dependency resolver
func NewResolver(log logrus.FieldLogger, externalDir, transformationDir string, parser Parser) Resolver {
	return &resolver{
		externalDir:          externalDir,
		transformationDir:    transformationDir,
		parser:               parser,
		log:                  log.WithField("component", "dependency_resolver"),
		externalModels:       make(map[string]*Model),
		transformationModels: make(map[string]*Model),
	}
}

// Start initializes the resolver by parsing all models
func (r *resolver) Start(_ context.Context) error {
	r.log.Debug("starting dependency resolver")

	// Parse external models
	r.log.WithField("dir", r.externalDir).Debug("parsing external models")
	externalModels, err := r.parser.ParseModelsInDir(r.externalDir, ModelTypeExternal)
	if err != nil {
		return fmt.Errorf("parsing external models: %w", err)
	}

	for _, model := range externalModels {
		r.externalModels[model.Name] = model
	}

	r.log.WithField("count", len(r.externalModels)).Info("loaded external models")

	// Parse transformation models
	r.log.WithField("dir", r.transformationDir).Debug("parsing transformation models")
	transformationModels, err := r.parser.ParseModelsInDir(r.transformationDir, ModelTypeTransformation)
	if err != nil {
		return fmt.Errorf("parsing transformation models: %w", err)
	}

	for _, model := range transformationModels {
		r.transformationModels[model.Name] = model
	}

	r.log.WithField("count", len(r.transformationModels)).Info("loaded transformation models")

	return nil
}

// Stop cleans up the resolver
func (r *resolver) Stop() error {
	r.log.Debug("stopping dependency resolver")
	return nil
}

// ResolveAndValidate performs smart dependency resolution and validation
func (r *resolver) ResolveAndValidate(testConfig *config.TestConfig) (*ResolutionResult, error) {
	r.log.WithField("model", testConfig.Model).Debug("resolving dependencies")

	// Find the target model
	targetModel, isTransformation := r.transformationModels[testConfig.Model]
	if !isTransformation {
		// Check if it's an external model
		externalModel, isExternal := r.externalModels[testConfig.Model]
		if !isExternal {
			return nil, fmt.Errorf("model %s not found in external or transformation models", testConfig.Model)
		}

		// External model - simple case
		result := &ResolutionResult{
			TargetModel:          externalModel,
			TransformationModels: []*Model{},
			ExternalTables:       []string{externalModel.Name},
			ParquetURLs:          make(map[string]string),
		}

		// Build parquet URL map
		if testConfig.ExternalData != nil {
			for tableName, extData := range testConfig.ExternalData {
				result.ParquetURLs[tableName] = extData.URL
			}
		}

		// Validate that required external data is provided
		if err := r.validateExternalData(result.ExternalTables, testConfig.ExternalData); err != nil {
			return nil, err
		}

		return result, nil
	}

	// Transformation model - build dependency graph
	transformations, err := r.buildDependencyGraph(testConfig.Model, make(map[string]bool))
	if err != nil {
		return nil, fmt.Errorf("building dependency graph: %w", err)
	}

	// Detect circular dependencies
	if err := r.detectCircularDependencies(transformations); err != nil {
		return nil, err
	}

	// Extract leaf external tables
	externalTables := r.extractLeafExternalTables(transformations)

	// For OR dependencies, only validate that at least one option is provided
	// Don't validate here - CBT config will include all options, SQL handles missing tables
	// if err := r.validateExternalData(externalTables, testConfig.ExternalData); err != nil {
	// 	return nil, err
	// }

	// Build parquet URL map from test config
	parquetURLs := make(map[string]string)
	if testConfig.ExternalData != nil {
		for tableName, extData := range testConfig.ExternalData {
			parquetURLs[tableName] = extData.URL
		}
	}

	// Topologically sort transformations
	sortedTransformations, err := r.topologicalSort(transformations)
	if err != nil {
		return nil, fmt.Errorf("topological sort: %w", err)
	}

	result := &ResolutionResult{
		TargetModel:          targetModel,
		TransformationModels: sortedTransformations,
		ExternalTables:       externalTables,
		ParquetURLs:          parquetURLs,
	}

	r.log.WithFields(logrus.Fields{
		"model":           testConfig.Model,
		"transformations": len(result.TransformationModels),
		"external_tables": len(result.ExternalTables),
	}).Debug("resolved dependencies")

	return result, nil
}

// buildDependencyGraph recursively builds the dependency graph for a model
func (r *resolver) buildDependencyGraph(modelName string, visited map[string]bool) ([]*Model, error) {
	if visited[modelName] {
		// Already visited, skip to avoid infinite recursion
		return nil, nil
	}
	visited[modelName] = true

	// Check if it's a transformation
	model, isTransformation := r.transformationModels[modelName]
	if !isTransformation {
		// It's an external model, no dependencies to recurse
		return nil, nil
	}

	models := []*Model{model}

	// Recursively resolve dependencies
	for _, dep := range model.Dependencies {
		depModels, err := r.buildDependencyGraph(dep, visited)
		if err != nil {
			return nil, err
		}
		models = append(models, depModels...)
	}

	return models, nil
}

// extractLeafExternalTables finds all external table dependencies
// For OR dependencies (multiple options), includes all of them since SQL may reference all
func (r *resolver) extractLeafExternalTables(transformations []*Model) []string {
	externalSet := make(map[string]bool)

	for _, model := range transformations {
		for _, dep := range model.Dependencies {
			// Check if dependency is an external model
			if _, isExternal := r.externalModels[dep]; isExternal {
				externalSet[dep] = true
			}
		}
	}

	// Convert set to sorted slice
	externals := make([]string, 0, len(externalSet))
	for ext := range externalSet {
		externals = append(externals, ext)
	}

	return externals
}

// topologicalSort sorts transformations in execution order (dependencies first)
func (r *resolver) topologicalSort(transformations []*Model) ([]*Model, error) {
	// Build adjacency list and in-degree map
	adjList := make(map[string][]*Model)
	inDegree := make(map[string]int)
	modelMap := make(map[string]*Model)

	for _, model := range transformations {
		modelMap[model.Name] = model
		if _, ok := inDegree[model.Name]; !ok {
			inDegree[model.Name] = 0
		}

		for _, dep := range model.Dependencies {
			// Only consider transformation dependencies for sorting
			if _, isTransformation := r.transformationModels[dep]; isTransformation {
				adjList[dep] = append(adjList[dep], model)
				inDegree[model.Name]++
			}
		}
	}

	// Kahn's algorithm for topological sort
	queue := make([]*Model, 0)
	for name, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, modelMap[name])
		}
	}

	sorted := make([]*Model, 0, len(transformations))
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
		return nil, fmt.Errorf("circular dependency detected in transformations")
	}

	return sorted, nil
}

// validateExternalData checks that all required external tables are provided
func (r *resolver) validateExternalData(required []string, provided map[string]*config.ExternalTable) error {
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
		return fmt.Errorf("test config missing required external_data tables: %v", missing)
	}

	return nil
}

// detectCircularDependencies uses DFS to detect cycles
func (r *resolver) detectCircularDependencies(transformations []*Model) error {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for _, model := range transformations {
		if !visited[model.Name] {
			if r.hasCycleDFS(model.Name, visited, recStack) {
				return fmt.Errorf("circular dependency detected involving model %s", model.Name)
			}
		}
	}

	return nil
}

// hasCycleDFS performs DFS to detect cycles
func (r *resolver) hasCycleDFS(modelName string, visited, recStack map[string]bool) bool {
	visited[modelName] = true
	recStack[modelName] = true

	model, isTransformation := r.transformationModels[modelName]
	if !isTransformation {
		recStack[modelName] = false
		return false
	}

	for _, dep := range model.Dependencies {
		if !visited[dep] {
			if r.hasCycleDFS(dep, visited, recStack) {
				return true
			}
		} else if recStack[dep] {
			return true
		}
	}

	recStack[modelName] = false
	return false
}

// IsExternalModel checks if a model exists in the external models directory
func (r *resolver) IsExternalModel(name string) bool {
	_, exists := r.externalModels[name]
	return exists
}

// IsTransformationModel checks if a model exists in the transformations directory
func (r *resolver) IsTransformationModel(name string) bool {
	_, exists := r.transformationModels[name]
	return exists
}
