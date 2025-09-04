package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/test"
	"github.com/spf13/cobra"
)

var (
	cleanInfra bool
)

var infraCmd = &cobra.Command{
	Use:   "infra",
	Short: "Manage ClickHouse infrastructure for development",
	Long: `Manage the ClickHouse infrastructure required for CBT development and testing.

This includes setting up the Xatu ClickHouse cluster and creating the necessary
network databases without starting the CBT engine or Redis services.`,
}

var infraSetupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Setup ClickHouse infrastructure",
	Long: `Setup the complete ClickHouse infrastructure for CBT development.

This command will:
1. Teardown any existing infrastructure (if --clean flag is used)
2. Clone the xatu repository (if not exists)
3. Start xatu with the clickhouse profile
4. Wait for ClickHouse migrations to complete
5. Setup the network database using xatu-cbt

The infrastructure will be ready for development but without the CBT engine running.`,
	RunE: func(_ *cobra.Command, _ []string) error {
		Logger.Info("Setting up ClickHouse infrastructure")

		ctx := context.Background()

		// Create docker manager for operations
		docker := test.NewDockerManager(Logger)

		// Phase 0: Teardown existing infrastructure if clean flag is set
		if cleanInfra {
			Logger.Info("Cleaning up any existing infrastructure...")
			teardownInfra(ctx, docker)
		}

		// Phase 1: Clone xatu repository if it doesn't exist
		xatuDir := "xatu"
		if _, err := os.Stat(xatuDir); os.IsNotExist(err) {
			xatuRef := os.Getenv("XATU_REF")
			if xatuRef == "" {
				xatuRef = "master"
			}

			Logger.WithField("ref", xatuRef).Info("Cloning xatu repository")
			if err := docker.CloneRepository(ctx, "https://github.com/ethpandaops/xatu", "xatu", xatuRef); err != nil {
				return fmt.Errorf("failed to clone xatu repository: %w", err)
			}
		} else {
			Logger.Info("Using existing xatu repository")
		}

		// Phase 2: Start xatu docker compose with clickhouse profile
		Logger.Info("Starting xatu ClickHouse cluster")
		if err := docker.ComposeUp(ctx, "xatu", []string{"clickhouse"}); err != nil {
			return fmt.Errorf("failed to start xatu docker compose: %w", err)
		}

		// Phase 3: Wait for migrations to complete
		Logger.Info("Waiting for ClickHouse migrations to complete (this may take a few minutes)...")
		if err := docker.WaitForContainerExit(ctx, "xatu-clickhouse-migrator", 60*time.Minute); err != nil {
			return fmt.Errorf("failed waiting for migrations: %w", err)
		}
		Logger.Info("ClickHouse migrations completed successfully")

		// Phase 4: Setup network database using xatu-cbt
		Logger.Info("Setting up network database")

		// Build xatu-cbt if not already built
		buildCmd := exec.Command("make", "build")
		if err := buildCmd.Run(); err != nil {
			Logger.WithError(err).Warn("Failed to build xatu-cbt, assuming it's already built")
		}

		// Run network teardown first to ensure clean state
		teardownCmd := exec.Command("./bin/xatu-cbt", "network", "teardown", "--force")
		teardownCmd.Env = os.Environ()
		if err := teardownCmd.Run(); err != nil {
			Logger.WithError(err).Debug("Network teardown failed (might not exist)")
		}

		// Run network setup
		setupCmd := exec.Command("./bin/xatu-cbt", "network", "setup", "--force")
		setupCmd.Env = os.Environ()
		if err := setupCmd.Run(); err != nil {
			// Handle replica already exists error
			if strings.Contains(err.Error(), "REPLICA_ALREADY_EXISTS") {
				Logger.Warn("Replica already exists, attempting cleanup and retry")

				// Force teardown
				forceTeardownCmd := exec.Command("./bin/xatu-cbt", "network", "teardown", "--force")
				forceTeardownCmd.Env = os.Environ()
				if teardownErr := forceTeardownCmd.Run(); teardownErr != nil {
					Logger.WithError(teardownErr).Warn("Failed to teardown during retry")
				}

				time.Sleep(2 * time.Second)

				// Retry setup
				retryCmd := exec.Command("./bin/xatu-cbt", "network", "setup", "--force")
				retryCmd.Env = os.Environ()
				if retryErr := retryCmd.Run(); retryErr != nil {
					return fmt.Errorf("failed to setup network database after retry: %w", retryErr)
				}
			} else {
				return fmt.Errorf("failed to setup network database: %w", err)
			}
		}

		fmt.Println("\n✅ ClickHouse infrastructure setup completed successfully!")
		fmt.Println("\nYou can now:")
		fmt.Println("  - Connect to ClickHouse at http://localhost:8123")
		fmt.Println("  - Run CBT models using: ./bin/xatu-cbt")
		fmt.Println("  - Start CBT engine if needed: docker compose up -d cbt-engine")
		fmt.Println("\nTo teardown the infrastructure, run: ./bin/xatu-cbt infra teardown")

		return nil
	},
}

var infraTeardownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Teardown ClickHouse infrastructure",
	Long: `Teardown the ClickHouse infrastructure.

This command will:
- Stop all local docker compose services
- Stop xatu docker compose services
- Remove the xatu directory
- Clean up all volumes`,
	RunE: func(_ *cobra.Command, _ []string) error {
		Logger.Info("Tearing down ClickHouse infrastructure")

		ctx := context.Background()
		docker := test.NewDockerManager(Logger)

		teardownInfra(ctx, docker)

		fmt.Println("\n✅ ClickHouse infrastructure teardown completed successfully!")
		return nil
	},
}

func teardownInfra(ctx context.Context, docker test.DockerManager) {
	// Stop local docker compose (if any services are running)
	if err := docker.ComposeDown(ctx, ".", true); err != nil {
		Logger.WithError(err).Debug("Failed to stop local docker compose (might not be running)")
	}

	// Clean up xatu directory if it exists
	xatuDir := "xatu"
	if _, err := os.Stat(xatuDir); err == nil {
		// Stop xatu docker compose
		Logger.Info("Stopping xatu ClickHouse cluster")
		if err := docker.ComposeDown(ctx, xatuDir, true); err != nil {
			Logger.WithError(err).Warn("Failed to stop xatu docker compose")
		}

		// Remove xatu directory
		Logger.Info("Removing xatu directory")
		if err := os.RemoveAll(xatuDir); err != nil {
			Logger.WithError(err).Warn("Failed to remove xatu directory")
		}
	}
}

func init() {
	infraSetupCmd.Flags().BoolVar(&cleanInfra, "clean", false, "Clean up existing infrastructure before setup")

	infraCmd.AddCommand(infraSetupCmd)
	infraCmd.AddCommand(infraTeardownCmd)
	rootCmd.AddCommand(infraCmd)
}
