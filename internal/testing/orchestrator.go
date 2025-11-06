package testing

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/testing/assertion"
	"github.com/ethpandaops/xatu-cbt/internal/testing/cache"
	"github.com/ethpandaops/xatu-cbt/internal/testing/cbt"
	testconfig "github.com/ethpandaops/xatu-cbt/internal/testing/config"
	"github.com/ethpandaops/xatu-cbt/internal/testing/database"
	"github.com/ethpandaops/xatu-cbt/internal/testing/dependency"
	"github.com/sirupsen/logrus"
)

// Orchestrator coordinates end-to-end test execution
type Orchestrator interface {
	Start(ctx context.Context) error
	Stop() error
	TestModel(ctx context.Context, network, spec, modelName string) (*TestResult, error)
	TestModels(ctx context.Context, network, spec string, modelNames []string, concurrency int) ([]*TestResult, error)
	TestSpec(ctx context.Context, network, spec string, concurrency int) ([]*TestResult, error)
}

// TestResult contains comprehensive test results for a single model
type TestResult struct {
	Model            string
	Network          string
	Spec             string
	ExternalTables   []string
	ParquetURLs      map[string]string
	Transformations  []string
	AssertionResults *assertion.RunResult
	Duration         time.Duration
	Success          bool
	Error            error
}

type orchestrator struct {
	configLoader    testconfig.Loader
	resolver        dependency.Resolver
	cache           cache.ParquetCache
	dbManager       database.Manager
	cbtEngine       cbt.Engine
	assertionRunner assertion.Runner
	log             logrus.FieldLogger

	// Track prepared network databases (key: "network")
	preparedNetworks   map[string]bool
	preparedNetworksMu sync.Mutex
}

// NewOrchestrator creates a new test orchestrator
func NewOrchestrator(
	log logrus.FieldLogger,
	configLoader testconfig.Loader,
	resolver dependency.Resolver,
	cache cache.ParquetCache,
	dbManager database.Manager,
	cbtEngine cbt.Engine,
	assertionRunner assertion.Runner,
) Orchestrator {
	return &orchestrator{
		configLoader:     configLoader,
		resolver:         resolver,
		cache:            cache,
		dbManager:        dbManager,
		cbtEngine:        cbtEngine,
		assertionRunner:  assertionRunner,
		log:              log.WithField("component", "test_orchestrator"),
		preparedNetworks: make(map[string]bool),
	}
}

// Start initializes all services
func (o *orchestrator) Start(ctx context.Context) error {
	o.log.Debug("starting test orchestrator")

	// Start dependency resolver
	if err := o.resolver.Start(ctx); err != nil {
		return fmt.Errorf("starting resolver: %w", err)
	}

	// Start parquet cache
	if err := o.cache.Start(ctx); err != nil {
		return fmt.Errorf("starting cache: %w", err)
	}

	// Start database manager
	if err := o.dbManager.Start(ctx); err != nil {
		return fmt.Errorf("starting database manager: %w", err)
	}

	// Start assertion runner
	if err := o.assertionRunner.Start(ctx); err != nil {
		return fmt.Errorf("starting assertion runner: %w", err)
	}

	// Start CBT engine
	if err := o.cbtEngine.Start(ctx); err != nil {
		return fmt.Errorf("starting cbt engine: %w", err)
	}

	o.log.Info("test orchestrator started")

	return nil
}

// Stop cleans up all services
func (o *orchestrator) Stop() error {
	o.log.Debug("stopping test orchestrator")

	var errs []error

	// Flush Redis to clean up any stale state
	o.log.Debug("flushing Redis cache on shutdown")
	flushCmd := exec.Command("docker", "exec", config.RedisContainerName, "redis-cli", "FLUSHALL")
	if err := flushCmd.Run(); err != nil {
		o.log.WithError(err).Warn("failed to flush redis on shutdown")
		errs = append(errs, fmt.Errorf("flushing redis: %w", err))
	}

	if err := o.cbtEngine.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping cbt engine: %w", err))
	}

	if err := o.assertionRunner.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping assertion runner: %w", err))
	}

	if err := o.dbManager.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping database manager: %w", err))
	}

	if err := o.cache.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping cache: %w", err))
	}

	if err := o.resolver.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping resolver: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors stopping orchestrator: %v", errs)
	}

	return nil
}

// TestModel tests a single model
func (o *orchestrator) TestModel(ctx context.Context, network, spec, modelName string) (*TestResult, error) {
	o.log.WithFields(logrus.Fields{
		"network": network,
		"spec":    spec,
		"model":   modelName,
	}).Info("testing model")

	// Load test config
	testConfig, err := o.configLoader.LoadForModel(spec, network, modelName)
	if err != nil {
		return nil, fmt.Errorf("loading test config: %w", err)
	}

	// Execute test
	return o.executeTest(ctx, testConfig)
}

// TestModels tests multiple models using grouped execution
// All tests for the same (network, spec) share one database and one CBT container
func (o *orchestrator) TestModels(ctx context.Context, network, spec string, modelNames []string, concurrency int) ([]*TestResult, error) {
	o.log.WithFields(logrus.Fields{
		"network":     network,
		"spec":        spec,
		"models":      modelNames,
		"concurrency": concurrency,
	}).Info("testing models")

	if concurrency <= 0 {
		concurrency = 1
	}

	// Prepare network database once before execution
	if err := o.ensureNetworkPrepared(ctx, network, spec, modelNames); err != nil {
		return nil, fmt.Errorf("preparing network database: %w", err)
	}

	// Load test configs for all models
	testConfigs := make([]*testconfig.TestConfig, 0, len(modelNames))
	for _, modelName := range modelNames {
		testConfig, err := o.configLoader.LoadForModel(spec, network, modelName)
		if err != nil {
			return nil, fmt.Errorf("loading test config for %s: %w", modelName, err)
		}
		testConfigs = append(testConfigs, testConfig)
	}

	// Execute all tests as a group (shared database, shared CBT container)
	return o.executeTestGroup(ctx, network, spec, testConfigs, concurrency)
}

// TestSpec tests all models in a spec using grouped execution
// All tests for a (network, spec) share one database and one CBT container
func (o *orchestrator) TestSpec(ctx context.Context, network, spec string, concurrency int) ([]*TestResult, error) {
	o.log.WithFields(logrus.Fields{
		"network":     network,
		"spec":        spec,
		"concurrency": concurrency,
	}).Info("testing spec")

	// Load all test configs for spec
	configs, err := o.configLoader.LoadForSpec(spec, network)
	if err != nil {
		return nil, fmt.Errorf("loading test configs: %w", err)
	}

	if concurrency <= 0 {
		concurrency = 1
	}

	// Extract model names for network preparation
	modelNames := make([]string, 0, len(configs))
	for modelName := range configs {
		modelNames = append(modelNames, modelName)
	}

	// Prepare network database once before execution
	if err := o.ensureNetworkPrepared(ctx, network, spec, modelNames); err != nil {
		return nil, fmt.Errorf("preparing network database: %w", err)
	}

	// Convert configs map to slice for consistent ordering
	testConfigs := make([]*testconfig.TestConfig, 0, len(configs))
	for _, cfg := range configs {
		testConfigs = append(testConfigs, cfg)
	}

	// Execute all tests as a group (shared database, shared CBT container)
	return o.executeTestGroup(ctx, network, spec, testConfigs, concurrency)
}

// executeTestGroup performs grouped test execution for a (network, spec)
// All tests share one database and one CBT container for optimal performance
func (o *orchestrator) executeTestGroup(ctx context.Context, network, spec string, testConfigs []*testconfig.TestConfig, concurrency int) ([]*TestResult, error) {
	o.log.WithFields(logrus.Fields{
		"network": network,
		"spec":    spec,
		"tests":   len(testConfigs),
	}).Info("executing test group")

	start := time.Now()

	// 1. Resolve dependencies for ALL models
	o.log.Info("resolving dependencies for all models in group")
	allExternalTables := make(map[string]bool)
	allTransformations := make(map[string]bool)
	allParquetURLs := make(map[string]string)
	resolutions := make(map[string]*dependency.ResolutionResult)

	for _, testConfig := range testConfigs {
		resolution, err := o.resolver.ResolveAndValidate(testConfig)
		if err != nil {
			return nil, fmt.Errorf("resolving dependencies for %s: %w", testConfig.Model, err)
		}
		resolutions[testConfig.Model] = resolution

		// Collect unique external tables (only if they're actual CBT external models)
		// Tables created by migrations (like dim_node) are filtered out
		for _, ext := range resolution.ExternalTables {
			if o.resolver.IsExternalModel(ext) {
				allExternalTables[ext] = true
			}
		}

		// Collect unique transformations
		for _, model := range resolution.TransformationModels {
			allTransformations[model.Name] = true
		}

		// Collect parquet URLs
		for table, url := range resolution.ParquetURLs {
			allParquetURLs[table] = url
		}
	}

	// Convert sets to slices
	externalTablesList := make([]string, 0, len(allExternalTables))
	for ext := range allExternalTables {
		externalTablesList = append(externalTablesList, ext)
	}

	transformationsList := make([]string, 0, len(allTransformations))
	for trans := range allTransformations {
		transformationsList = append(transformationsList, trans)
	}

	// Extract list of models explicitly being tested (not dependencies)
	testModelsList := make([]string, 0, len(testConfigs))
	for _, testConfig := range testConfigs {
		testModelsList = append(testModelsList, testConfig.Model)
	}

	o.log.WithFields(logrus.Fields{
		"external_tables":  len(externalTablesList),
		"transformations":  len(transformationsList),
		"test_models":      len(testModelsList),
	}).Info("dependencies resolved for all models")

	// 2. Create ONE test database in CBT cluster
	o.log.Info("creating shared test database in cbt cluster")
	dbName, err := o.dbManager.CreateTestDatabase(ctx, network, spec)
	if err != nil {
		return nil, fmt.Errorf("creating test database: %w", err)
	}

	// Ensure cleanup on exit
	defer func() {
		if err := o.dbManager.DropDatabase(context.Background(), dbName); err != nil {
			o.log.WithError(err).WithField("database", dbName).Error("failed to drop test database")
		}
	}()

	o.log.WithField("database", dbName).Info("shared test database created in cbt cluster")

	// 3. Run transformations in CBT cluster (if any)
	if len(transformationsList) > 0 {
		o.log.WithFields(logrus.Fields{
			"transformations":  len(transformationsList),
			"external_tables": len(externalTablesList),
		}).Info("running transformations in cbt cluster")

		// Combine all external models and transformations for CBT config
		// This tells CBT about all dependencies so it can run them
		allModels := append(externalTablesList, transformationsList...)

		// IMPORTANT: Clear Redis cache BEFORE starting CBT
		o.log.Info("clearing Redis cache before starting CBT")
		if err := o.flushRedisCache(ctx); err != nil {
			o.log.WithError(err).Warn("failed to clear Redis cache (non-fatal)")
		}

		// Start single CBT container
		// Pass allModels for config (so CBT knows about all dependencies)
		// Wait for transformationsList (all transformation models including dependencies)
		// NOT testModelsList which may include external models that aren't transformations
		if err := o.cbtEngine.RunTransformations(ctx, network, dbName, allModels, transformationsList); err != nil {
			return nil, fmt.Errorf("running transformations: %w", err)
		}

		o.log.Info("all transformations completed")
	}

	// 4. Run ALL assertions in parallel using shared database
	o.log.WithField("tests", len(testConfigs)).Info("running assertions for all tests in parallel")

	results := make([]*TestResult, len(testConfigs))
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	// Use worker pool to run assertions with controlled concurrency
	type assertionJob struct {
		index      int
		testConfig *testconfig.TestConfig
	}

	jobs := make(chan assertionJob, len(testConfigs))

	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobs {
				testConfig := job.testConfig
				resolution := resolutions[testConfig.Model]

				o.log.WithFields(logrus.Fields{
					"worker": workerID,
					"model":  testConfig.Model,
				}).Debug("worker running assertions")

				result := &TestResult{
					Model:             testConfig.Model,
					Network:           network,
					Spec:              spec,
					ExternalTables:    resolution.ExternalTables,
					ParquetURLs:       resolution.ParquetURLs,
					Transformations:   extractModelNames(resolution.TransformationModels),
				}

				// Run assertions for this test
				assertionResults, err := o.assertionRunner.RunAssertions(ctx, dbName, testConfig.Assertions)
				if err != nil {
					result.Error = fmt.Errorf("running assertions: %w", err)
					result.Success = false
				} else {
					result.AssertionResults = assertionResults
					result.Success = assertionResults.Failed == 0
				}

				result.Duration = time.Since(start)

				resultsMu.Lock()
				results[job.index] = result
				resultsMu.Unlock()

				if !result.Success {
					o.log.WithField("model", testConfig.Model).Warn("model test failed")
				}
			}
		}(i)
	}

	// Queue assertion jobs
	for i, testConfig := range testConfigs {
		jobs <- assertionJob{index: i, testConfig: testConfig}
	}
	close(jobs)

	// Wait for all assertions to complete
	wg.Wait()

	o.log.WithFields(logrus.Fields{
		"network":  network,
		"spec":     spec,
		"tests":    len(results),
		"duration": time.Since(start),
	}).Info("test group execution completed")

	return results, nil
}

// executeTest performs the core test execution logic
func (o *orchestrator) executeTest(ctx context.Context, testConfig *testconfig.TestConfig) (*TestResult, error) {
	o.log.WithField("model", testConfig.Model).Debug("executing test")
	start := time.Now()

	result := &TestResult{
		Model:   testConfig.Model,
		Network: testConfig.Network,
		Spec:    testConfig.Spec,
	}

	// 1. Resolve dependencies and validate completeness
	o.log.Info("resolving dependencies")
	resolution, err := o.resolver.ResolveAndValidate(testConfig)
	if err != nil {
		result.Error = fmt.Errorf("resolving dependencies: %w", err)
		result.Duration = time.Since(start)
		return result, result.Error
	}

	result.ExternalTables = resolution.ExternalTables
	result.ParquetURLs = resolution.ParquetURLs
	result.Transformations = extractModelNames(resolution.TransformationModels)

	o.log.WithFields(logrus.Fields{
		"external_tables":  len(result.ExternalTables),
		"transformations":  len(result.Transformations),
	}).Debug("dependencies resolved")

	// 2. Check if network database preparation is needed (single model test path)
	// For parallel tests, this is done once in ensureNetworkPrepared()
	o.preparedNetworksMu.Lock()
	needsPreparation := !o.preparedNetworks[testConfig.Network]
	if needsPreparation {
		o.preparedNetworks[testConfig.Network] = true
	}
	o.preparedNetworksMu.Unlock()

	if needsPreparation {
		// 3. Fetch/cache parquet files (only if we need to load data)
		o.log.WithField("count", len(resolution.ParquetURLs)).Info("fetching parquet files")
		localPaths := make(map[string]string, len(resolution.ParquetURLs))
		for tableName, url := range resolution.ParquetURLs {
			o.log.WithField("table", tableName).Info("fetching parquet file")
			path, err := o.cache.Get(ctx, url, tableName)
			if err != nil {
				result.Error = fmt.Errorf("fetching parquet file for %s: %w", tableName, err)
				result.Duration = time.Since(start)
				return result, result.Error
			}
			o.log.WithFields(logrus.Fields{
				"table": tableName,
				"path":  path,
			}).Info("parquet file ready")
			localPaths[tableName] = path
		}

		o.log.WithField("files", len(localPaths)).Debug("parquet files ready")

		// 4. Prepare network database in xatu cluster
		o.log.WithField("network", testConfig.Network).Info("preparing network database in xatu cluster")
		if err := o.dbManager.PrepareNetworkDatabase(ctx, testConfig.Network); err != nil {
			result.Error = fmt.Errorf("preparing network database: %w", err)
			result.Duration = time.Since(start)
			return result, result.Error
		}

		// 5. Load parquet data into xatu cluster network database
		o.log.WithField("network", testConfig.Network).Info("loading parquet data into xatu cluster")
		if err := o.dbManager.LoadParquetData(ctx, testConfig.Network, localPaths); err != nil {
			result.Error = fmt.Errorf("loading parquet data: %w", err)
			result.Duration = time.Since(start)
			return result, result.Error
		}

		o.log.WithField("network", testConfig.Network).Info("network database ready in xatu cluster")
	} else {
		o.log.WithField("network", testConfig.Network).Debug("network database already prepared, skipping parquet validation")
	}

	// 5. Create ephemeral test database in CBT cluster (transformations only)
	o.log.Info("creating test database in cbt cluster")
	dbName, err := o.dbManager.CreateTestDatabase(ctx, testConfig.Network, testConfig.Spec)
	if err != nil {
		result.Error = fmt.Errorf("creating test database: %w", err)
		result.Duration = time.Since(start)
		return result, result.Error
	}

	// Ensure cleanup on failure
	defer func() {
		if err := o.dbManager.DropDatabase(context.Background(), dbName); err != nil {
			o.log.WithError(err).WithField("database", dbName).Error("failed to drop test database")
		}
	}()

	o.log.WithField("database", dbName).Debug("test database created in cbt cluster")

	// 6. Run transformations in CBT cluster (queries xatu cluster via cluster() function)
	if len(result.Transformations) > 0 {
		o.log.WithFields(logrus.Fields{
			"transformations":  len(result.Transformations),
			"external_tables": len(result.ExternalTables),
		}).Info("running transformations in cbt cluster")

		// Include external models in CBT config (filtering out tables created by migrations)
		// Only include external tables that actually have CBT model files
		externalModels := make([]string, 0, len(result.ExternalTables))
		for _, ext := range result.ExternalTables {
			if o.resolver.IsExternalModel(ext) {
				externalModels = append(externalModels, ext)
			} else {
				o.log.WithField("table", ext).Debug("skipping non-model table (created by migrations)")
			}
		}

		o.log.WithFields(logrus.Fields{
			"total_externals":  len(result.ExternalTables),
			"model_externals": len(externalModels),
		}).Debug("including external models for CBT config")

		// Combine all external models and transformations for CBT config
		allModels := append(externalModels, result.Transformations...)

		// IMPORTANT: Clear Redis cache BEFORE starting CBT
		// CBT performs initial scans immediately on startup and caches results
		// If we don't clear cache first, it will read stale values from previous runs
		o.log.Info("clearing Redis cache before starting CBT")
		if err := o.flushRedisCache(ctx); err != nil {
			o.log.WithError(err).Warn("failed to clear Redis cache (non-fatal)")
		}

		// Pass allModels for config (so CBT knows about all dependencies)
		// Wait for result.Transformations (all transformation models including dependencies)
		// This ensures we wait for dependencies to complete, not just the target model
		if err := o.cbtEngine.RunTransformations(ctx, testConfig.Network, dbName, allModels, result.Transformations); err != nil {
			result.Error = fmt.Errorf("running transformations: %w", err)
			result.Duration = time.Since(start)
			return result, result.Error
		}

		o.log.Debug("transformations completed")
	}

	// 7. Run assertions
	o.log.Info("running assertions")
	assertionResults, err := o.assertionRunner.RunAssertions(ctx, dbName, testConfig.Assertions)
	if err != nil {
		result.Error = fmt.Errorf("running assertions: %w", err)
		result.Duration = time.Since(start)
		return result, result.Error
	}

	result.AssertionResults = assertionResults
	result.Success = assertionResults.Failed == 0
	result.Duration = time.Since(start)

	o.log.WithFields(logrus.Fields{
		"model":      testConfig.Model,
		"success":    result.Success,
		"assertions": assertionResults.Total,
		"passed":     assertionResults.Passed,
		"failed":     assertionResults.Failed,
		"duration":   result.Duration,
	}).Info("test completed")

	return result, nil
}

// extractModelNames extracts model names from Model structs
func extractModelNames(models []*dependency.Model) []string {
	names := make([]string, 0, len(models))
	for _, model := range models {
		names = append(names, model.Name)
	}
	return names
}

// ensureNetworkPrepared ensures the network database is prepared with all required parquet data
// This is called once before parallel test execution to avoid redundant setup
func (o *orchestrator) ensureNetworkPrepared(ctx context.Context, network, spec string, modelNames []string) error {
	o.preparedNetworksMu.Lock()
	alreadyPrepared := o.preparedNetworks[network]
	if alreadyPrepared {
		o.preparedNetworksMu.Unlock()
		o.log.WithField("network", network).Debug("network database already prepared, skipping")
		return nil
	}
	o.preparedNetworks[network] = true
	o.preparedNetworksMu.Unlock()

	o.log.WithField("network", network).Info("preparing network database for all models")

	// Collect all external tables and parquet URLs across all models
	allExternalTables := make(map[string]bool)
	allParquetURLs := make(map[string]string)

	for _, modelName := range modelNames {
		testConfig, err := o.configLoader.LoadForModel(spec, network, modelName)
		if err != nil {
			return fmt.Errorf("loading test config for %s: %w", modelName, err)
		}

		resolution, err := o.resolver.ResolveAndValidate(testConfig)
		if err != nil {
			return fmt.Errorf("resolving dependencies for %s: %w", modelName, err)
		}

		for _, table := range resolution.ExternalTables {
			allExternalTables[table] = true
		}

		for table, url := range resolution.ParquetURLs {
			allParquetURLs[table] = url
		}
	}

	// Fetch all parquet files
	o.log.WithField("count", len(allParquetURLs)).Info("fetching parquet files for network")
	localPaths := make(map[string]string, len(allParquetURLs))
	for tableName, url := range allParquetURLs {
		o.log.WithField("table", tableName).Debug("fetching parquet file")
		path, err := o.cache.Get(ctx, url, tableName)
		if err != nil {
			return fmt.Errorf("fetching parquet file for %s: %w", tableName, err)
		}
		localPaths[tableName] = path
	}

	// Prepare network database in xatu cluster
	o.log.WithField("network", network).Info("preparing network database in xatu cluster")
	if err := o.dbManager.PrepareNetworkDatabase(ctx, network); err != nil {
		return fmt.Errorf("preparing network database: %w", err)
	}

	// Load parquet data into xatu cluster network database
	o.log.WithField("network", network).Info("loading parquet data into xatu cluster")
	if err := o.dbManager.LoadParquetData(ctx, network, localPaths); err != nil {
		return fmt.Errorf("loading parquet data: %w", err)
	}

	o.log.WithField("network", network).Info("network database ready in xatu cluster")

	return nil
}

// flushRedisCache clears Redis cache to prevent stale cached values
func (o *orchestrator) flushRedisCache(ctx context.Context) error {
	// Use docker exec to flush Redis database
	// This is simpler than managing a Redis client connection
	cmd := exec.CommandContext(ctx, "docker", "exec", config.RedisContainerName, "redis-cli", "FLUSHDB")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("flushing redis: %w (output: %s)", err, string(output))
	}

	o.log.Debug("Redis cache flushed successfully")
	return nil
}
