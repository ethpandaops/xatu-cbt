package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/testing"
	"github.com/ethpandaops/xatu-cbt/internal/testing/assertion"
	"github.com/ethpandaops/xatu-cbt/internal/testing/cache"
	"github.com/ethpandaops/xatu-cbt/internal/testing/cbt"
	"github.com/ethpandaops/xatu-cbt/internal/testing/database"
	"github.com/ethpandaops/xatu-cbt/internal/testing/dependency"
	"github.com/ethpandaops/xatu-cbt/internal/testing/metrics"
	"github.com/ethpandaops/xatu-cbt/internal/testing/testcfg"
	"github.com/ethpandaops/xatu-cbt/internal/testing/testdef"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	errTestsFailed    = fmt.Errorf("some tests failed")
	testNetwork       string
	testSpec          string
	testTimeout       time.Duration
	testVerbose       bool
	testCacheDir      string
	testCacheSize     int64
	testConcurrency   int
	testForceRebuild  bool
	testCleanupTestDB bool
	xatuClickhouseURL string
	cbtClickhouseURL  string
	redisURL          string
	xatuRepoURL       string
	xatuRef           string
)

// testCmd represents the test command
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Run CBT transformation tests",
	Long: `Execute tests for CBT transformations with dependency resolution.

This command runs tests against a persistent ClickHouse cluster using ephemeral
test databases for complete isolation. Tests automatically resolve dependencies
and only load required parquet files.`,
	SilenceUsage: true,
}

// testModelsCmd tests specific models
var testModelsCmd = &cobra.Command{
	Use:   "models [model1,model2,...]",
	Short: "Test specific models with dependency resolution",
	Long: `Test one or more specific models.

Models are specified as a comma-separated list. Each model test:
- Resolves dependencies automatically
- Loads only required parquet files (cached locally)
- Creates ephemeral test database
- Runs transformations in topological order
- Executes assertions
- Cleans up database

Example:
  xatu-cbt test models fct_block --spec pectra --network mainnet
  xatu-cbt test models fct_block,fct_attestation --spec pectra`,
	Args:         cobra.ExactArgs(1),
	RunE:         runTestModels,
	SilenceUsage: true,
}

// testSpecCmd tests all models in a spec
var testSpecCmd = &cobra.Command{
	Use:   "spec",
	Short: "Test all models in a spec",
	Long: `Test all models in a specification.

Loads all model test configs from tests/{network}/{spec}/models/ and executes
them sequentially. Results are aggregated at the end.

Example:
  xatu-cbt test spec --spec pectra --network mainnet
  xatu-cbt test spec --spec fusaka --network sepolia`,
	RunE:         runTestSpec,
	SilenceUsage: true,
}

// runTestsWithConfig sets up the orchestrator, runs tests, and returns results.
// The testRunner function is called to execute the actual tests.
// Output is handled by the orchestrator's internal formatter.
func runTestsWithConfig(
	ctx context.Context,
	cmd *cobra.Command,
	_ int,
	testRunner func(testing.Orchestrator) ([]*testing.TestResult, error),
) error {
	orchestrator, err := setupOrchestrator(cmd)
	if err != nil {
		return fmt.Errorf("setting up orchestrator: %w", err)
	}

	setupCleanupHandler(orchestrator)
	defer func() {
		if stopErr := orchestrator.Stop(); stopErr != nil {
			logrus.WithError(stopErr).Error("error stopping orchestrator")
		}
	}()

	if startErr := orchestrator.Start(ctx); startErr != nil {
		return fmt.Errorf("starting orchestrator: %w", startErr)
	}

	results, err := testRunner(orchestrator)
	if err != nil {
		return fmt.Errorf("running tests: %w", err)
	}

	if hasFailures(results) {
		return errTestsFailed
	}

	return nil
}

func init() {
	testCmd.AddCommand(testModelsCmd)
	testCmd.AddCommand(testSpecCmd)
	testCmd.PersistentFlags().StringVar(&testNetwork, "network", "mainnet", "Network name (mainnet, sepolia)")
	testCmd.PersistentFlags().StringVar(&testSpec, "spec", "pectra", "Spec name (pectra, fusaka)")
	testCmd.PersistentFlags().DurationVar(&testTimeout, "timeout", 30*time.Minute, "Test timeout")
	testCmd.PersistentFlags().BoolVar(&testVerbose, "verbose", false, "Verbose output")
	testCmd.PersistentFlags().StringVar(&testCacheDir, "cache-dir", getDefaultCacheDir(), "Parquet cache directory")
	testCmd.PersistentFlags().Int64Var(&testCacheSize, "cache-max-size", 10*1024*1024*1024, "Max cache size in bytes (10GB)")
	testCmd.PersistentFlags().IntVar(&testConcurrency, "concurrency", 10, "Number of tests to run in parallel")
	testCmd.PersistentFlags().BoolVar(&testForceRebuild, "force-rebuild", false, "Force rebuild of xatu cluster (clear tables and re-run migrations)")
	testCmd.PersistentFlags().BoolVar(&testCleanupTestDB, "cleanup-test-db", false, "Cleanup test database on completion (useful for CI, disabled by default for debugging)")
	testCmd.PersistentFlags().StringVar(&xatuClickhouseURL, "xatu-clickhouse-url", config.GetXatuClickHouseURL(), "Xatu ClickHouse cluster URL (external data)")
	testCmd.PersistentFlags().StringVar(&cbtClickhouseURL, "cbt-clickhouse-url", config.GetCBTClickHouseURL(), "CBT ClickHouse cluster URL (transformations)")
	testCmd.PersistentFlags().StringVar(&redisURL, "redis-url", config.DefaultRedisURL, "Redis connection URL")
	testCmd.PersistentFlags().StringVar(&xatuRepoURL, "xatu-repo", config.XatuRepoURL, "Xatu repository URL")
	testCmd.PersistentFlags().StringVar(&xatuRef, "xatu-ref", config.XatuDefaultRef, "Xatu repository ref (branch/tag/commit)")
}

func runTestModels(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	modelNames := strings.Split(args[0], ",")
	for i, name := range modelNames {
		modelNames[i] = strings.TrimSpace(name)
	}

	return runTestsWithConfig(ctx, cmd, len(modelNames), func(orchestrator testing.Orchestrator) ([]*testing.TestResult, error) {
		return orchestrator.TestModels(ctx, testNetwork, testSpec, modelNames, testConcurrency)
	})
}

func runTestSpec(cmd *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	return runTestsWithConfig(ctx, cmd, 0, func(orchestrator testing.Orchestrator) ([]*testing.TestResult, error) {
		return orchestrator.TestSpec(ctx, testNetwork, testSpec, testConcurrency)
	})
}

func setupOrchestrator(_ *cobra.Command) (testing.Orchestrator, error) {
	log := newLogger(testVerbose)

	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("getting working directory: %w", err)
	}

	xatuRepoPath, err := ensureXatuRepo(log, wd, xatuRepoURL, xatuRef)
	if err != nil {
		return nil, err
	}

	xatuMigrationDir := filepath.Join(xatuRepoPath, config.XatuMigrationsPath)
	metricsCollector := metrics.NewCollector(log)
	testConfig := testcfg.DefaultTestConfig()
	configLoader := testdef.NewLoader(log, filepath.Join(wd, config.TestsDir))
	parser := dependency.NewParser(log)
	resolver := dependency.NewResolver(
		log,
		filepath.Join(wd, config.ModelsExternalDir),
		filepath.Join(wd, config.ModelsTransformationsDir),
		parser,
	)
	parquetCache := cache.NewParquetCache(log, testConfig, testCacheDir, testCacheSize, metricsCollector)
	migrationRunner := database.NewMigrationRunner(log, filepath.Join(wd, config.MigrationsDir), "")
	dbManager := database.NewManager(log, testConfig, xatuClickhouseURL, cbtClickhouseURL, migrationRunner, xatuMigrationDir, testForceRebuild)
	configGen := cbt.NewConfigGenerator(
		log,
		filepath.Join(wd, config.ModelsExternalDir),
		filepath.Join(wd, config.ModelsTransformationsDir),
		cbtClickhouseURL,
		redisURL,
	)
	cbtEngine := cbt.NewEngine(log, testConfig, configGen, cbtClickhouseURL, redisURL, filepath.Join(wd, config.ModelsDir))
	assertionRunner := assertion.NewRunner(log, cbtClickhouseURL, 5, 30*time.Second)

	orchestrator := testing.NewOrchestrator(
		log,
		testVerbose,
		testCleanupTestDB,
		os.Stdout,
		metricsCollector,
		configLoader,
		resolver,
		parquetCache,
		dbManager,
		cbtEngine,
		assertionRunner,
	)

	return orchestrator, nil
}

// setupCleanupHandler sets up signal handling for graceful cleanup on Ctrl+C.
func setupCleanupHandler(orchestrator testing.Orchestrator) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logrus.Warn("\nReceived interrupt signal, cleaning up...")
		if err := orchestrator.Stop(); err != nil {
			logrus.WithError(err).Error("error stopping orchestrator during cleanup")
		}
		os.Exit(130) // Exit code 130 = 128 + SIGINT(2)
	}()
}

func hasFailures(results []*testing.TestResult) bool {
	for _, result := range results {
		if !result.Success {
			return true
		}
	}
	return false
}

func getDefaultCacheDir() string {
	return ".parquet_cache"
}
