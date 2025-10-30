package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/test"
	"github.com/spf13/cobra"
)

var (
	skipSetup bool
)

var testCmd = &cobra.Command{
	Use:   "test [network/spec]",
	Short: "Run a test suite",
	Long: `Run a test suite with setup, data ingestion, and assertions.

Test name format: network/spec (e.g., "mainnet/pectra", "sepolia/fusaka")

Test directory structure:
  tests/
  ├── mainnet/
  │   └── pectra/
  │       ├── data/
  │       └── assertions/
  └── sepolia/
      └── fusaka/
          ├── data/
          └── assertions/

The test command will:
1. Run setup phase (unless --skip-setup is used):
   - Stop existing docker containers
   - Clone xatu repository
   - Start xatu with clickhouse profile
   - Setup network databases
   - Ingest test data from YAML files
   - Start CBT engine
2. Run assertions on interval until all pass or timeout

Examples:
  ./bin/xatu-cbt test mainnet/pectra
  ./bin/xatu-cbt test sepolia/fusaka`,
	Args: cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		testName := args[0]

		// Create test service using the shared logger
		cfg := test.Config{
			TestsDir:      "tests",
			XatuRepoURL:   "https://github.com/ethpandaops/xatu",
			XatuRef:       "", // Will use XATU_REF env var or default to master
			Timeout:       10 * time.Minute,
			CheckInterval: 10 * time.Second,
		}

		svc := test.NewService(Logger, cfg)

		// Start the service
		ctx := context.Background()
		if err := svc.Start(ctx); err != nil {
			return fmt.Errorf("failed to start test service: %w", err)
		}
		defer func() {
			if err := svc.Stop(); err != nil {
				Logger.WithError(err).Warn("Failed to stop test service")
			}
		}()

		// Run the test
		if err := svc.RunTest(ctx, testName, skipSetup); err != nil {
			return fmt.Errorf("test failed: %w", err)
		}

		fmt.Printf("\n✅ Test '%s' passed successfully!\n", testName)
		return nil
	},
}

var testTeardownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Teardown test environment",
	Long: `Teardown the test environment by stopping all containers and cleaning up.

This command will:
- Stop local docker compose with volumes
- Stop xatu docker compose if it exists
- Remove xatu directory if it exists`,
	RunE: func(_ *cobra.Command, _ []string) error {
		// Create test service using the shared logger
		cfg := test.Config{
			TestsDir: "tests",
		}

		svc := test.NewService(Logger, cfg)

		// Run teardown
		ctx := context.Background()
		if err := svc.Teardown(ctx); err != nil {
			return fmt.Errorf("teardown failed: %w", err)
		}

		fmt.Println("\n✅ Test environment teardown completed successfully!")
		return nil
	},
}

func init() {
	testCmd.Flags().BoolVar(&skipSetup, "skip-setup", false, "Skip the setup phase and run assertions only")
	testCmd.AddCommand(testTeardownCmd)
	rootCmd.AddCommand(testCmd)
}
