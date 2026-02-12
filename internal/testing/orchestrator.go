// Package testing provides end-to-end test orchestration and execution.
package testing

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/testing/assertion"
	"github.com/ethpandaops/xatu-cbt/internal/testing/output"
	"github.com/ethpandaops/xatu-cbt/internal/testing/testdef"
	"github.com/sirupsen/logrus"
)

// TestResult contains comprehensive test results for a single model.
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

// OrchestratorConfig contains configuration for test orchestration.
type OrchestratorConfig struct {
	Logger           logrus.FieldLogger
	Verbose          bool
	CleanupTestDB    bool
	Writer           io.Writer
	MetricsCollector Collector
	ConfigLoader     testdef.Loader
	ModelCache       *ModelCache
	ParquetCache     *ParquetCache
	DBManager        *DatabaseManager
	CBTEngine        *CBTEngine
	AssertionRunner  assertion.Runner // For CBT cluster (transformation models)
	XatuAssertion    assertion.Runner // For Xatu cluster (external models)
	MigrationDir     string
}

// Orchestrator coordinates end-to-end test execution.
// This is the concrete implementation without an interface abstraction.
type Orchestrator struct {
	configLoader    testdef.Loader
	modelCache      *ModelCache
	cache           *ParquetCache
	dbManager       *DatabaseManager
	cbtEngine       *CBTEngine
	assertionRunner assertion.Runner // For CBT cluster (transformation models)
	xatuAssertion   assertion.Runner // For Xatu cluster (external models)
	migrationDir    string
	log             logrus.FieldLogger
	metrics         Collector
	formatter       *output.Formatter
	verbose         bool
	cleanupTestDB   bool

	// Template database tracking for per-test isolation
	templatesPrepared bool
	templatesMu       sync.Mutex

	// Atomic counter for generating unique test IDs.
	// Prevents collisions when consecutive generateTestID() calls occur within the same nanosecond.
	testIDCounter atomic.Uint64
}

// metricsAdapter adapts Collector to output.MetricsProvider.
type metricsAdapter struct {
	collector Collector
}

func (m *metricsAdapter) GetParquetMetrics() []output.ParquetLoadMetric {
	metrics := m.collector.GetParquetMetrics()
	result := make([]output.ParquetLoadMetric, len(metrics))
	for i, metric := range metrics {
		result[i] = output.ParquetLoadMetric{
			Table:     metric.Table,
			Source:    output.ParquetLoadSource(metric.Source),
			SizeBytes: metric.SizeBytes,
			Duration:  metric.Duration,
			Timestamp: metric.Timestamp,
		}
	}
	return result
}

func (m *metricsAdapter) GetTestMetrics() []output.TestResultMetric {
	metrics := m.collector.GetTestMetrics()
	result := make([]output.TestResultMetric, len(metrics))
	for i, metric := range metrics {
		failedAssertions := make([]output.FailedAssertionDetail, len(metric.FailedAssertions))
		for j, fa := range metric.FailedAssertions {
			failedAssertions[j] = output.FailedAssertionDetail{
				Name:     fa.Name,
				Expected: fa.Expected,
				Actual:   fa.Actual,
				Error:    fa.Error,
			}
		}
		result[i] = output.TestResultMetric{
			Model:            metric.Model,
			Passed:           metric.Passed,
			Duration:         metric.Duration,
			AssertionsTotal:  metric.AssertionsTotal,
			AssertionsPassed: metric.AssertionsPassed,
			AssertionsFailed: metric.AssertionsFailed,
			ErrorMessage:     metric.ErrorMessage,
			FailedAssertions: failedAssertions,
			Timestamp:        metric.Timestamp,
		}
	}
	return result
}

func (m *metricsAdapter) GetSummary() output.SummaryMetric {
	summary := m.collector.GetSummary()
	return output.SummaryMetric{
		TotalDuration: summary.TotalDuration,
		TotalTests:    summary.TotalTests,
		PassedTests:   summary.PassedTests,
		FailedTests:   summary.FailedTests,
		CacheHits:     summary.CacheHits,
		CacheMisses:   summary.CacheMisses,
		CacheHitRate:  summary.CacheHitRate,
		TotalDataSize: summary.TotalDataSize,
	}
}

// NewOrchestrator creates a new test orchestrator.
func NewOrchestrator(cfg *OrchestratorConfig) *Orchestrator {
	writer := cfg.Writer
	if writer == nil {
		writer = os.Stdout
	}

	// Wrap metrics collector in an adapter for the output package
	adapter := &metricsAdapter{collector: cfg.MetricsCollector}

	outputFormatter := output.NewFormatter(
		cfg.Logger,
		writer,
		cfg.Verbose,
		adapter,
	)

	return &Orchestrator{
		configLoader:    cfg.ConfigLoader,
		modelCache:      cfg.ModelCache,
		cache:           cfg.ParquetCache,
		dbManager:       cfg.DBManager,
		cbtEngine:       cfg.CBTEngine,
		assertionRunner: cfg.AssertionRunner,
		xatuAssertion:   cfg.XatuAssertion,
		migrationDir:    cfg.MigrationDir,
		log:             cfg.Logger.WithField("component", "test_orchestrator"),
		metrics:         cfg.MetricsCollector,
		formatter:       outputFormatter,
		verbose:         cfg.Verbose,
		cleanupTestDB:   cfg.CleanupTestDB,
	}
}

// Start initializes the orchestrator and all its components.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.log.Debug("starting test orchestrator")

	if err := o.metrics.Start(ctx); err != nil {
		return fmt.Errorf("starting metrics collector: %w", err)
	}

	if err := o.cache.Start(ctx); err != nil {
		return fmt.Errorf("starting cache: %w", err)
	}

	if err := o.dbManager.Start(ctx); err != nil {
		return fmt.Errorf("starting database manager: %w", err)
	}

	if err := o.assertionRunner.Start(ctx); err != nil {
		return fmt.Errorf("starting CBT assertion runner: %w", err)
	}

	if err := o.xatuAssertion.Start(ctx); err != nil {
		return fmt.Errorf("starting xatu assertion runner: %w", err)
	}

	if err := o.cbtEngine.Start(ctx); err != nil {
		return fmt.Errorf("starting cbt engine: %w", err)
	}

	// Flush Redis once at startup to clear stale tasks from previous runs.
	// This must happen BEFORE any tests run, not per-test (which would race).
	if err := o.flushRedisCache(ctx); err != nil {
		o.log.WithError(err).Warn("failed to flush Redis at startup (non-fatal)")
	}

	o.log.Info("test orchestrator started")

	return nil
}

// Stop cleans up all orchestrator resources and flushes Redis cache once.
func (o *Orchestrator) Stop() error {
	o.log.Debug("stopping test orchestrator")

	var errs []error

	// Flush Redis cache once at shutdown
	//nolint:gosec // G204: Docker command with trusted container name
	flushCmd := exec.Command("docker", "exec", config.RedisContainerName, "redis-cli", "FLUSHALL")
	if err := flushCmd.Run(); err != nil {
		o.log.WithError(err).Warn("failed to flush redis cache")
		errs = append(errs, fmt.Errorf("flushing redis: %w", err))
	}

	// Stop all components in reverse order of start
	if err := o.cbtEngine.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping cbt engine: %w", err))
	}

	if err := o.assertionRunner.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping CBT assertion runner: %w", err))
	}

	if err := o.xatuAssertion.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping xatu assertion runner: %w", err))
	}

	if err := o.dbManager.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping database manager: %w", err))
	}

	if err := o.cache.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping cache: %w", err))
	}

	if err := o.metrics.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stopping metrics collector: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors stopping orchestrator: %v", errs) //nolint:err113 // Include error list for debugging
	}

	return nil
}

// TestModels tests multiple models using grouped execution.
// All tests for the same (network, spec) share one database and one CBT container.
func (o *Orchestrator) TestModels(
	ctx context.Context,
	network, spec string,
	modelNames []string,
	concurrency int,
) ([]*TestResult, error) {
	o.log.WithFields(logrus.Fields{
		"network":     network,
		"spec":        spec,
		"models":      modelNames,
		"concurrency": concurrency,
	}).Info("testing models")

	if concurrency <= 0 {
		concurrency = 1
	}

	// Load test configs for all models
	testConfigs := make([]*testdef.TestDefinition, 0, len(modelNames))
	for _, modelName := range modelNames {
		testConfig, err := o.configLoader.LoadForModel(spec, network, modelName)
		if err != nil {
			return nil, fmt.Errorf("loading test config for %s: %w", modelName, err)
		}

		testConfigs = append(testConfigs, testConfig)
	}

	return o.executeTestGroup(ctx, network, spec, testConfigs, concurrency)
}

// TestSpec tests all models in a spec using grouped execution.
// All tests for a (network, spec) share one database and one CBT container.
func (o *Orchestrator) TestSpec(ctx context.Context, network, spec string, concurrency int) ([]*TestResult, error) {
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

	// Convert configs map to slice for consistent ordering
	testConfigs := make([]*testdef.TestDefinition, 0, len(configs))
	for _, cfg := range configs {
		testConfigs = append(testConfigs, cfg)
	}

	return o.executeTestGroup(ctx, network, spec, testConfigs, concurrency)
}

// preclonedDBs holds pre-cloned database names and resolved dependencies for a test.
type preclonedDBs struct {
	extDB string
	cbtDB string
	deps  *Dependencies
}

// executeTestGroup performs test execution with per-test database isolation.
// Each test gets its own cloned databases to prevent data conflicts.
func (o *Orchestrator) executeTestGroup(
	ctx context.Context,
	network, spec string,
	testConfigs []*testdef.TestDefinition,
	concurrency int,
) ([]*TestResult, error) {
	start := time.Now()

	o.log.WithFields(logrus.Fields{
		"network":     network,
		"spec":        spec,
		"tests":       len(testConfigs),
		"concurrency": concurrency,
	}).Info("starting test group with per-test isolation")

	// Step 1: Ensure template databases are prepared (migrations run once)
	if err := o.ensureTemplatesPrepared(ctx, network); err != nil {
		return nil, fmt.Errorf("preparing templates: %w", err)
	}

	// Step 2: Pre-clone ALL databases in parallel (both ext and cbt for each test)
	o.log.WithField("tests", len(testConfigs)).Info("pre-cloning all test databases in parallel")
	cloneStart := time.Now()

	testDBs, err := o.precloneAllDatabases(ctx, testConfigs)
	if err != nil {
		return nil, fmt.Errorf("pre-cloning databases: %w", err)
	}

	o.log.WithFields(logrus.Fields{
		"tests":    len(testDBs),
		"duration": time.Since(cloneStart),
	}).Info("all test databases pre-cloned")

	// Ensure cleanup of all pre-cloned databases
	defer o.cleanupPreclonedDatabases(ctx, testDBs)

	// Step 3: Run tests in parallel with worker pool (DBs already cloned)
	results := make([]*TestResult, 0, len(testConfigs))
	resultChan := make(chan *TestResult, len(testConfigs))
	sem := make(chan struct{}, concurrency)

	var wg sync.WaitGroup

	for _, testCfg := range testConfigs {
		wg.Add(1)

		go func(cfg *testdef.TestDefinition) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// Get pre-cloned databases and deps for this test
			dbs := testDBs[cfg.Model]

			// Execute test with pre-cloned databases and pre-resolved deps
			result := o.executeTestWithDBs(ctx, network, spec, cfg, dbs.extDB, dbs.cbtDB, dbs.deps)
			resultChan <- result
		}(testCfg)
	}

	// Close result channel when all tests complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for result := range resultChan {
		results = append(results, result)
	}

	o.log.WithFields(logrus.Fields{
		"network":  network,
		"spec":     spec,
		"tests":    len(results),
		"duration": time.Since(start),
	}).Info("all tests completed")

	o.formatter.PrintParquetSummary()
	o.formatter.PrintTestResults()
	o.formatter.PrintSummary()

	return results, nil
}

// precloneAllDatabases clones all test databases in parallel upfront.
// It resolves dependencies first to clone only the needed tables per test.
func (o *Orchestrator) precloneAllDatabases(
	ctx context.Context,
	testConfigs []*testdef.TestDefinition,
) (map[string]*preclonedDBs, error) {
	testDBs := make(map[string]*preclonedDBs, len(testConfigs))
	var mu sync.Mutex

	// Step 1: Resolve dependencies for all tests upfront (fast, no I/O)
	for _, cfg := range testConfigs {
		deps, err := o.modelCache.ResolveTestDependencies(cfg)
		if err != nil {
			return nil, fmt.Errorf("resolving dependencies for %s: %w", cfg.Model, err)
		}

		testDBs[cfg.Model] = &preclonedDBs{deps: deps}
	}

	// Step 2: Clone databases in parallel with only needed tables
	var wg sync.WaitGroup

	errChan := make(chan error, len(testConfigs)*2) // 2 clones per test

	// Limit parallel clone operations to avoid overwhelming ClickHouse
	const maxConcurrentClones = 15
	cloneSem := make(chan struct{}, maxConcurrentClones)

	for _, cfg := range testConfigs {
		testID := o.generateTestID()
		model := cfg.Model
		deps := testDBs[model].deps

		// Merge external table refs from model deps + test definition's ExternalData.
		// Build a map keyed by model name to deduplicate and enrich with cross-database info.
		refMap := make(map[string]ExternalTableRef, len(deps.ExternalTableRefs))
		for _, ref := range deps.ExternalTableRefs {
			refMap[ref.ModelName] = ref
		}

		for tableName := range cfg.ExternalData {
			if _, exists := refMap[tableName]; !exists {
				// Table from test config without model metadata — use defaults (standard database)
				ref := ExternalTableRef{ModelName: tableName}
				if ext := o.modelCache.GetExternalModel(tableName); ext != nil {
					ref.SourceDB = ext.SourceDB
					ref.SourceTable = ext.SourceTable
				}

				refMap[tableName] = ref
			}
		}

		extRefs := make([]ExternalTableRef, 0, len(refMap))
		for _, ref := range refMap {
			extRefs = append(extRefs, ref)
		}

		// Clone external DB with only needed tables
		wg.Add(1)

		go func(m, id string, refs []ExternalTableRef) {
			defer wg.Done()

			cloneSem <- struct{}{}
			defer func() { <-cloneSem }()

			extDB, err := o.dbManager.CloneExternalDatabase(ctx, id, refs)
			if err != nil {
				errChan <- fmt.Errorf("cloning ext DB for %s: %w", m, err)

				return
			}

			mu.Lock()
			testDBs[m].extDB = extDB
			mu.Unlock()
		}(model, testID, extRefs)

		// Clone CBT DB with only needed tables (transformation model names)
		wg.Add(1)

		go func(m, id string, transformations []*ModelMetadata) {
			defer wg.Done()

			cloneSem <- struct{}{}
			defer func() { <-cloneSem }()

			// Extract table names from transformation models
			cbtTables := extractModelNames(transformations)

			cbtDB, err := o.dbManager.CloneCBTDatabase(ctx, id, cbtTables)
			if err != nil {
				errChan <- fmt.Errorf("cloning cbt DB for %s: %w", m, err)

				return
			}

			mu.Lock()
			testDBs[m].cbtDB = cbtDB
			mu.Unlock()
		}(model, testID, deps.TransformationModels)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		o.cleanupPreclonedDatabases(ctx, testDBs)

		return nil, err
	}

	return testDBs, nil
}

// cleanupPreclonedDatabases drops all pre-cloned databases.
func (o *Orchestrator) cleanupPreclonedDatabases(ctx context.Context, testDBs map[string]*preclonedDBs) {
	if !o.cleanupTestDB {
		return
	}

	for model, dbs := range testDBs {
		if dbs.extDB != "" {
			if err := o.dbManager.DropExternalDatabase(ctx, dbs.extDB); err != nil {
				o.log.WithError(err).WithField("model", model).Warn("failed to drop external database")
			}
		}

		if dbs.cbtDB != "" {
			if err := o.dbManager.DropCBTDatabase(ctx, dbs.cbtDB); err != nil {
				o.log.WithError(err).WithField("model", model).Warn("failed to drop CBT database")
			}
		}
	}
}

// executeTestWithDBs runs a test using pre-cloned databases and pre-resolved dependencies.
// This is used when databases are pre-cloned upfront for all tests.
// Cleanup is handled at the group level, not per-test.
func (o *Orchestrator) executeTestWithDBs(
	ctx context.Context,
	network, spec string,
	testConfig *testdef.TestDefinition,
	extDB, cbtDB string,
	deps *Dependencies,
) *TestResult {
	start := time.Now()

	logCtx := o.log.WithFields(logrus.Fields{
		"model":  testConfig.Model,
		"ext_db": extDB,
		"cbt_db": cbtDB,
	})
	logCtx.Info("executing test")

	result := &TestResult{
		Model:   testConfig.Model,
		Network: network,
		Spec:    spec,
	}

	// Ensure metrics are recorded for ALL cases (including early errors)
	defer func() {
		result.Duration = time.Since(start)
		o.recordTestMetrics(result, testConfig)

		switch {
		case result.Error != nil:
			logCtx.WithError(result.Error).Error("test failed")
		case !result.Success:
			logCtx.Error("assertion(s) failed")
		default:
			logCtx.WithField("duration", result.Duration).Info("test completed successfully")
		}
	}()

	// Use pre-resolved dependencies
	result.ExternalTables = deps.ExternalTables
	result.ParquetURLs = deps.ParquetURLs
	result.Transformations = extractModelNames(deps.TransformationModels)

	// Step 2: Fetch and load parquet data into pre-cloned external database
	if loadErr := o.fetchAndLoadParquetData(ctx, extDB, deps.ParquetURLs); loadErr != nil {
		result.Error = loadErr
		return result
	}

	// Step 3: Run transformations (reads extDB, writes cbtDB)
	if transformErr := o.runTestTransformations(ctx, network, cbtDB, extDB, deps); transformErr != nil {
		result.Error = transformErr
		return result
	}

	// Step 4: Run assertions against the appropriate database and cluster
	var (
		assertionResults *assertion.RunResult
		assertErr        error
	)

	if len(deps.TransformationModels) == 0 {
		// External model test - use Xatu cluster runner with extDB
		assertionResults, assertErr = o.xatuAssertion.RunAssertions(ctx, testConfig.Model, extDB, testConfig.Assertions)
	} else {
		// Transformation model test - use CBT cluster runner with cbtDB
		assertionResults, assertErr = o.assertionRunner.RunAssertions(ctx, testConfig.Model, cbtDB, testConfig.Assertions)
	}

	if assertErr != nil {
		result.Error = fmt.Errorf("running assertions: %w", assertErr)
		result.Success = false

		return result
	}

	result.AssertionResults = assertionResults
	result.Success = assertionResults.Failed == 0

	return result
}

// fetchAndLoadParquetData fetches parquet files from cache and loads them into the database.
func (o *Orchestrator) fetchAndLoadParquetData(
	ctx context.Context,
	database string,
	parquetURLs map[string]string,
) error {
	localPaths := make(map[string]string, len(parquetURLs))

	for tableName, url := range parquetURLs {
		path, err := o.cache.Get(ctx, url, tableName)
		if err != nil {
			return fmt.Errorf("fetching parquet file for %s: %w", tableName, err)
		}

		localPaths[tableName] = path
	}

	if err := o.dbManager.LoadParquetData(ctx, database, localPaths); err != nil {
		return fmt.Errorf("loading parquet data: %w", err)
	}

	return nil
}

// runTestTransformations runs CBT transformations for the test.
// Skips if there are no transformation models (external model tests).
func (o *Orchestrator) runTestTransformations(
	ctx context.Context,
	network, cbtDB, extDB string,
	deps *Dependencies,
) error {
	// Skip for external model tests - no transformations to run
	if len(deps.TransformationModels) == 0 {
		return nil
	}

	transformationNames := extractModelNames(deps.TransformationModels)
	allModels := make([]string, 0, len(deps.ExternalTables)+len(transformationNames))
	allModels = append(allModels, deps.ExternalTables...)
	allModels = append(allModels, transformationNames...)

	if err := o.cbtEngine.RunTransformations(ctx, network, cbtDB, extDB, allModels, transformationNames); err != nil {
		return fmt.Errorf("running transformations: %w", err)
	}

	return nil
}

// generateTestID creates a unique identifier for a test execution.
// Uses an atomic counter combined with timestamp to guarantee uniqueness even when
// consecutive calls occur within the same nanosecond (common in tight loops).
func (o *Orchestrator) generateTestID() string {
	counter := o.testIDCounter.Add(1)
	timestamp := time.Now().UnixNano()

	return fmt.Sprintf("%d_%d", timestamp, counter)
}

// recordTestMetrics records metrics for a test result.
func (o *Orchestrator) recordTestMetrics(result *TestResult, testConfig *testdef.TestDefinition) {
	var (
		errorMessage     string
		assertionsTotal  = 0
		assertionsPassed = 0
		assertionsFailed = 0
		failedAssertions = make([]FailedAssertionDetail, 0)
	)

	if result.Error != nil {
		errorMessage = result.Error.Error()
	}

	if result.AssertionResults != nil {
		assertionsTotal = result.AssertionResults.Total
		assertionsPassed = result.AssertionResults.Passed
		assertionsFailed = result.AssertionResults.Failed

		// Extract failed assertion details
		for _, assertionResult := range result.AssertionResults.Results {
			if !assertionResult.Passed {
				var errMsg string
				if assertionResult.Error != nil {
					errMsg = assertionResult.Error.Error()
				}

				failedAssertions = append(failedAssertions, FailedAssertionDetail{
					Name:     assertionResult.Name,
					Expected: assertionResult.Expected,
					Actual:   assertionResult.Actual,
					Error:    errMsg,
				})
			}
		}
	}

	o.metrics.RecordTestResult(&TestResultMetric{
		Model:            testConfig.Model,
		Passed:           result.Success,
		Duration:         result.Duration,
		AssertionsTotal:  assertionsTotal,
		AssertionsPassed: assertionsPassed,
		AssertionsFailed: assertionsFailed,
		ErrorMessage:     errorMessage,
		FailedAssertions: failedAssertions,
		Timestamp:        time.Now(),
	})
}

// extractModelNames extracts model names from ModelMetadata structs.
func extractModelNames(models []*ModelMetadata) []string {
	names := make([]string, 0, len(models))
	for _, model := range models {
		names = append(names, model.Name)
	}

	return names
}

// ensureTemplatesPrepared runs both xatu and xatu-cbt migrations once to create template databases.
// Per-test databases will be cloned from these templates to avoid re-running migrations.
func (o *Orchestrator) ensureTemplatesPrepared(ctx context.Context, network string) error {
	o.templatesMu.Lock()
	defer o.templatesMu.Unlock()

	if o.templatesPrepared {
		o.log.Debug("templates already prepared, skipping")
		return nil
	}

	o.log.Info("preparing template databases in parallel")

	// Run both migrations in parallel - they're on different clusters
	var (
		wg      sync.WaitGroup
		errChan = make(chan error, 2)
	)

	// Xatu migrations (external model template)
	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := o.dbManager.PrepareNetworkDatabase(ctx, network); err != nil {
			errChan <- fmt.Errorf("creating xatu template: %w", err)
		}
	}()

	// CBT migrations (transformation template)
	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := o.dbManager.CreateCBTTemplate(ctx, o.migrationDir); err != nil {
			errChan <- fmt.Errorf("creating CBT template: %w", err)
		}
	}()

	wg.Wait()
	close(errChan)

	// Return first error if any
	for err := range errChan {
		return err
	}

	o.templatesPrepared = true
	o.log.Info("template databases ready")

	return nil
}

// flushRedisCache clears Redis cache to prevent stale cached values.
func (o *Orchestrator) flushRedisCache(ctx context.Context) error {
	// Use docker exec to flush Redis database
	//nolint:gosec // G204: Docker command with trusted container name.
	cmd := exec.CommandContext(ctx, "docker", "exec", config.RedisContainerName, "redis-cli", "FLUSHDB")
	if cmdOutput, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("flushing redis: %w (output: %s)", err, string(cmdOutput))
	}

	o.log.Debug("flushed redis cache")

	return nil
}
