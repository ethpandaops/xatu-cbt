// Package testing provides end-to-end test orchestration and execution.
package testing

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/testing/assertion"
	"github.com/ethpandaops/xatu-cbt/internal/testing/cache"
	"github.com/ethpandaops/xatu-cbt/internal/testing/cbt"
	"github.com/ethpandaops/xatu-cbt/internal/testing/testdef"
	"github.com/ethpandaops/xatu-cbt/internal/testing/database"
	"github.com/ethpandaops/xatu-cbt/internal/testing/dependency"
	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/ethpandaops/xatu-cbt/internal/testing/output"
	"github.com/ethpandaops/xatu-cbt/internal/testing/table"
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

// Orchestrator coordinates end-to-end test execution.
type Orchestrator interface {
	Start(ctx context.Context) error
	Stop() error
	TestModels(ctx context.Context, network, spec string, modelNames []string, concurrency int) ([]*TestResult, error)
	TestSpec(ctx context.Context, network, spec string, concurrency int) ([]*TestResult, error)
}

type orchestrator struct {
	configLoader       testdef.Loader
	resolver           dependency.Resolver
	cache              cache.ParquetCache
	dbManager          database.Manager
	cbtEngine          cbt.Engine
	assertionRunner    assertion.Runner
	log                logrus.FieldLogger
	metrics            metrics.Collector
	formatter          output.Formatter
	verbose            bool
	preparedNetworks   map[string]bool
	preparedNetworksMu sync.Mutex
}

// NewOrchestrator creates a new test orchestrator.
func NewOrchestrator(
	log logrus.FieldLogger,
	verbose bool,
	writer io.Writer,
	metricsCollector metrics.Collector,
	configLoader testdef.Loader,
	resolver dependency.Resolver,
	parquetCache cache.ParquetCache,
	dbManager database.Manager,
	cbtEngine cbt.Engine,
	assertionRunner assertion.Runner,
) Orchestrator {
	var (
		tableRenderer    = table.NewRenderer(log)
		parquetFormatter = table.NewParquetFormatter(log, tableRenderer)
		resultsFormatter = table.NewResultsFormatter(log, tableRenderer)
		summaryFormatter = table.NewSummaryFormatter(log, tableRenderer)
	)

	if writer == nil {
		writer = os.Stdout
	}

	outputFormatter := output.NewFormatter(
		writer,
		verbose,
		metricsCollector,
		tableRenderer,
		parquetFormatter,
		resultsFormatter,
		summaryFormatter,
	)

	return &orchestrator{
		configLoader:     configLoader,
		resolver:         resolver,
		cache:            parquetCache,
		dbManager:        dbManager,
		cbtEngine:        cbtEngine,
		assertionRunner:  assertionRunner,
		log:              log.WithField("component", "test_orchestrator"),
		metrics:          metricsCollector,
		formatter:        outputFormatter,
		verbose:          verbose,
		preparedNetworks: make(map[string]bool),
	}
}

func (o *orchestrator) Start(ctx context.Context) error {
	o.log.Debug("starting test orchestrator")

	if err := o.metrics.Start(ctx); err != nil {
		return fmt.Errorf("starting metrics collector: %w", err)
	}

	if err := o.resolver.Start(ctx); err != nil {
		return fmt.Errorf("starting resolver: %w", err)
	}

	if err := o.cache.Start(ctx); err != nil {
		return fmt.Errorf("starting cache: %w", err)
	}

	if err := o.dbManager.Start(ctx); err != nil {
		return fmt.Errorf("starting database manager: %w", err)
	}

	if err := o.assertionRunner.Start(ctx); err != nil {
		return fmt.Errorf("starting assertion runner: %w", err)
	}

	if err := o.cbtEngine.Start(ctx); err != nil {
		return fmt.Errorf("starting cbt engine: %w", err)
	}

	o.log.Info("test orchestrator started")

	return nil
}

func (o *orchestrator) Stop() error {
	o.log.Debug("stopping test orchestrator")

	var errs []error

	//nolint:gosec // G204: Docker command with trusted container name
	flushCmd := exec.Command("docker", "exec", config.RedisContainerName, "redis-cli", "FLUSHALL")
	if err := flushCmd.Run(); err != nil {
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
func (o *orchestrator) TestModels(
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

	// Load test configs for all models.
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

	// Convert configs map to slice for consistent ordering
	testConfigs := make([]*testdef.TestDefinition, 0, len(configs))
	for _, cfg := range configs {
		testConfigs = append(testConfigs, cfg)
	}

	return o.executeTestGroup(ctx, network, spec, testConfigs, concurrency)
}

// executeTestGroup performs grouped test execution for a (network, spec)
// All tests share one database and one CBT container for optimal performance
//
//nolint:gocyclo // Complex test orchestration with multiple coordination steps.
func (o *orchestrator) executeTestGroup(
	ctx context.Context,
	network, spec string,
	testConfigs []*testdef.TestDefinition,
	concurrency int,
) ([]*TestResult, error) {
	var (
		logCtx             = o.log.WithField("cluster", "xatu-cbt")
		start              = time.Now()
		allExternalTables  = make(map[string]bool)
		allTransformations = make(map[string]bool)
		allParquetURLs     = make(map[string]string)
		resolutions        = make(map[string]*dependency.ResolutionResult)
	)

	// Resolve dependencies for ALL models.
	for _, testConfig := range testConfigs {
		resolution, err := o.resolver.ResolveAndValidate(testConfig)
		if err != nil {
			return nil, fmt.Errorf("resolving dependencies for %s: %w", testConfig.Model, err)
		}

		resolutions[testConfig.Model] = resolution

		// Collect unique external tables.
		for _, ext := range resolution.ExternalTables {
			if o.resolver.IsExternalModel(ext) {
				allExternalTables[ext] = true
			}
		}

		// Collect unique transformations.
		for _, model := range resolution.TransformationModels {
			allTransformations[model.Name] = true
		}

		// Collect parquet URLs.
		for table, url := range resolution.ParquetURLs {
			allParquetURLs[table] = url
		}
	}

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

	logCtx.WithFields(logrus.Fields{
		"external_tables": len(externalTablesList),
		"transformations": len(transformationsList),
		"test_models":     len(testModelsList),
	}).Info("resolved dependencies for models")

	// Prepare network database in xatu cluster
	if err := o.ensureNetworkDatabaseReady(ctx, network, allParquetURLs); err != nil {
		return nil, fmt.Errorf("preparing network database: %w", err)
	}

	// Create ONE test database in CBT cluster.
	dbName, err := o.dbManager.CreateTestDatabase(ctx, network, spec)
	if err != nil {
		return nil, fmt.Errorf("creating test database: %w", err)
	}

	// Ensure cleanup on exit.
	defer func() {
		if err := o.dbManager.DropDatabase(context.Background(), dbName); err != nil {
			o.log.WithError(err).WithField("database", dbName).Error("failed to drop test database")
		}
	}()

	// Run transformations in CBT cluster.
	if len(transformationsList) > 0 {
		// Combine all external models and transformations for CBT config.
		// This tells CBT about all dependencies so it can run them.
		allModels := make([]string, 0, len(externalTablesList)+len(transformationsList))
		allModels = append(allModels, externalTablesList...)
		allModels = append(allModels, transformationsList...)

		// Clear Redis cache BEFORE starting CBT. Clears anything stale from
		// previous runs on the host.
		if err := o.flushRedisCache(ctx); err != nil {
			o.log.WithError(err).Warn("failed to clear Redis cache (non-fatal)")
		}

		// Start single CBT container.
		// Pass allModels for config (so CBT knows about all dependencies).
		// Wait for transformationsList (all transformation models including dependencies).
		if err := o.cbtEngine.RunTransformations(ctx, network, dbName, allModels, transformationsList); err != nil {
			return nil, fmt.Errorf("running transformations: %w", err)
		}
	}

	if !o.verbose {
		o.formatter.PrintParquetSummary()
	}

	// Run ALL assertions in parallel using shared database.
	o.log.WithField("tests", len(testConfigs)).Info("running assertions")

	// Use worker pool to run assertions with controlled concurrency.
	type assertionJob struct {
		index      int
		testConfig *testdef.TestDefinition
	}

	var (
		results   = make([]*TestResult, len(testConfigs))
		resultsMu sync.Mutex
		wg        sync.WaitGroup
		jobs      = make(chan assertionJob, len(testConfigs))
	)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				testConfig := job.testConfig
				resolution := resolutions[testConfig.Model]

				result := &TestResult{
					Model:           testConfig.Model,
					Network:         network,
					Spec:            spec,
					ExternalTables:  resolution.ExternalTables,
					ParquetURLs:     resolution.ParquetURLs,
					Transformations: extractModelNames(resolution.TransformationModels),
				}

				// Run assertions for this test.
				assertionResults, err := o.assertionRunner.RunAssertions(ctx, dbName, testConfig.Assertions)
				if err != nil {
					result.Error = fmt.Errorf("running assertions: %w", err)
					result.Success = false
				} else {
					result.AssertionResults = assertionResults
					result.Success = assertionResults.Failed == 0
				}

				result.Duration = time.Since(start)

				// Record test result metrics.
				var (
					errorMessage     string
					assertionsTotal  = 0
					assertionsPassed = 0
					assertionsFailed = 0
					failedAssertions = make([]metrics.FailedAssertionDetail, 0)
				)

				if result.Error != nil {
					errorMessage = result.Error.Error()
				}

				if result.AssertionResults != nil {
					assertionsTotal = result.AssertionResults.Total
					assertionsPassed = result.AssertionResults.Passed
					assertionsFailed = result.AssertionResults.Failed

					// Extract failed assertion details.
					for _, assertionResult := range result.AssertionResults.Results {
						if !assertionResult.Passed {
							var errMsg string

							if assertionResult.Error != nil {
								errMsg = assertionResult.Error.Error()
							}

							failedAssertions = append(failedAssertions, metrics.FailedAssertionDetail{
								Name:     assertionResult.Name,
								Expected: assertionResult.Expected,
								Actual:   assertionResult.Actual,
								Error:    errMsg,
							})
						}
					}
				}

				o.metrics.RecordTestResult(&metrics.TestResultMetric{
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

				resultsMu.Lock()
				results[job.index] = result
				resultsMu.Unlock()

				if !result.Success {
					o.log.WithField("model", testConfig.Model).Error("assertion(s) failed")
				}
			}
		}()
	}

	// Queue assertion jobs.
	for i, testConfig := range testConfigs {
		jobs <- assertionJob{index: i, testConfig: testConfig}
	}
	close(jobs)

	// Wait for all assertions to complete.
	wg.Wait()

	o.log.WithFields(logrus.Fields{
		"network":  network,
		"spec":     spec,
		"tests":    len(results),
		"duration": time.Since(start),
	}).Info("all assertions completed")

	if !o.verbose {
		o.formatter.PrintTestResults()
	}

	o.formatter.PrintSummary()

	return results, nil
}

// extractModelNames extracts model names from Model structs
func extractModelNames(models []*dependency.Model) []string {
	names := make([]string, 0, len(models))
	for _, model := range models {
		names = append(names, model.Name)
	}

	return names
}

// ensureNetworkDatabaseReady prepares the network database with parquet data if needed
func (o *orchestrator) ensureNetworkDatabaseReady(ctx context.Context, network string, allParquetURLs map[string]string) error {
	// Check if network is already prepared.
	o.preparedNetworksMu.Lock()
	alreadyPrepared := o.preparedNetworks[network]
	if alreadyPrepared {
		o.preparedNetworksMu.Unlock()
		o.log.WithField("network", network).Debug("network database already prepared, skipping")
		return nil
	}
	o.preparedNetworks[network] = true
	o.preparedNetworksMu.Unlock()

	// Network needs preparation - fetch parquet files and load data.
	xatuLogCtx := o.log.WithField("cluster", "xatu")

	xatuLogCtx.WithFields(logrus.Fields{
		"count": len(allParquetURLs),
	}).Info("fetching parquet files")

	localPaths := make(map[string]string, len(allParquetURLs))
	for tableName, url := range allParquetURLs {
		path, err := o.cache.Get(ctx, url, tableName)
		if err != nil {
			return fmt.Errorf("fetching parquet file for %s: %w", tableName, err)
		}

		localPaths[tableName] = path
	}

	// Prepare network database in xatu cluster.
	xatuLogCtx.Info("preparing network database")
	if err := o.dbManager.PrepareNetworkDatabase(ctx, network); err != nil {
		return fmt.Errorf("preparing network database: %w", err)
	}

	// Load parquet data into xatu cluster network database.
	xatuLogCtx.Info("loading parquet data")
	if err := o.dbManager.LoadParquetData(ctx, network, localPaths); err != nil {
		return fmt.Errorf("loading parquet data: %w", err)
	}

	return nil
}

// flushRedisCache clears Redis cache to prevent stale cached values
func (o *orchestrator) flushRedisCache(ctx context.Context) error {
	// Use docker exec to flush Redis database.
	//nolint:gosec // G204: Docker command with trusted container name.
	cmd := exec.CommandContext(ctx, "docker", "exec", config.RedisContainerName, "redis-cli", "FLUSHDB")
	if cmdOutput, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("flushing redis: %w (output: %s)", err, string(cmdOutput))
	}

	o.log.Debug("flushed redis cache")

	return nil
}
