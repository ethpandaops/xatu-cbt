package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ethpandaops/xatu-cbt/pkg/config"
	"github.com/ethpandaops/xatu-cbt/pkg/infra"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// Infra command flags
	infraCleanupTestDBs bool
	infraVerbose        bool
	infraClickhouseURL  string
	infraRedisURL       string
)

// infraCmd represents the infrastructure command
var infraCmd = &cobra.Command{
	Use:   "infra",
	Short: "Manage platform infrastructure",
	Long: `Start, stop, and manage the persistent ClickHouse cluster platform.

This command provides a stable interface for managing the shared platform infrastructure
used by both testing and development workflows. External repositories (e.g., ethpandaops/lab)
can call these commands to orchestrate the full development stack.

The platform includes:
- ClickHouse cluster (2-node cluster with Zookeeper)
- Redis (state management)
- Shared across tests (ephemeral databases) and development (persistent databases)`,
}

// infraStartCmd starts the platform
var infraStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start platform infrastructure",
	Long: `Starts the platform infrastructure (ClickHouse cluster, Zookeeper, Redis).

This platform is shared across:
- Tests: Uses ephemeral test databases (test_mainnet_pectra_*)
- Development: Uses persistent databases (mainnet, sepolia)
- External repos: Can be orchestrated from ethpandaops/lab or other projects

Example:
  xatu-cbt infra start

  # From external repo
  cd ../xatu-cbt && ./xatu-cbt infra start`,
	RunE: runInfraStart,
}

// infraStopCmd stops the platform
var infraStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop platform infrastructure",
	Long: `Stops the platform infrastructure and optionally cleans up ephemeral test databases.

Example:
  xatu-cbt infra stop
  xatu-cbt infra stop --cleanup-test-dbs=false`,
	RunE: runInfraStop,
}

// infraStatusCmd checks platform status
var infraStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check platform status",
	Long: `Reports platform health, container status, and lists active databases.

Example:
  xatu-cbt infra status
  xatu-cbt infra status --verbose`,
	RunE: runInfraStatus,
}

// infraResetCmd resets the platform (removes volumes)
var infraResetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset platform infrastructure (removes all volumes)",
	Long: `Stops the platform infrastructure and removes all volumes for a complete clean slate.

This is useful when:
- You need to test fresh xatu migrations
- Disk space is running low
- You want to completely reset the test environment

Example:
  xatu-cbt infra reset`,
	RunE: runInfraReset,
}

func init() {
	// Add infra subcommands
	infraCmd.AddCommand(infraStartCmd)
	infraCmd.AddCommand(infraStopCmd)
	infraCmd.AddCommand(infraStatusCmd)
	infraCmd.AddCommand(infraResetCmd)

	// Connection flags
	infraCmd.PersistentFlags().StringVar(&infraClickhouseURL, "clickhouse-url", config.DefaultCBTClickHouseURL, "CBT ClickHouse cluster URL")
	infraCmd.PersistentFlags().StringVar(&infraRedisURL, "redis-url", config.DefaultRedisURL, "Redis connection URL")

	// Stop command flags
	infraStopCmd.Flags().BoolVar(&infraCleanupTestDBs, "cleanup-test-dbs", true, "Cleanup ephemeral test databases")

	// Status command flags
	infraStatusCmd.Flags().BoolVar(&infraVerbose, "verbose", false, "Show detailed container and database information")
}

// createInfraManagers creates and returns Docker and ClickHouse managers with the shared configuration.
func createInfraManagers(log logrus.FieldLogger) (infra.DockerManager, infra.ClickHouseManager) {
	dockerManager := infra.NewDockerManager(
		config.PlatformComposeFile,
		config.ProjectName,
		log,
	)

	chManager := infra.NewClickHouseManager(
		dockerManager,
		infraClickhouseURL,
		log,
	)

	return dockerManager, chManager
}

func runInfraStart(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup logger
	log := newLogger(false)

	fmt.Println("Starting platform infrastructure...")

	// Get working directory
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting working directory: %w", err)
	}

	// Ensure xatu repository exists (needed for xatu-clickhouse configs)
	fmt.Println("Ensuring xatu repository...")
	if _, err := ensureXatuRepo(wd, xatuRepoURL, xatuRef, log); err != nil {
		return err
	}

	// Create infrastructure managers
	_, chManager := createInfraManagers(log)

	// Start infrastructure
	if err := chManager.Start(ctx); err != nil {
		return fmt.Errorf("starting infrastructure: %w", err)
	}

	fmt.Println("\n✓ Platform infrastructure started successfully")
	fmt.Printf("\nCBT ClickHouse URL: %s\n", infraClickhouseURL)
	fmt.Printf("Redis URL: %s\n", infraRedisURL)
	fmt.Printf("\nReady for testing and development!\n")

	return nil
}

func runInfraStop(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup logger
	log := newLogger(false)

	fmt.Println("Stopping platform infrastructure...")

	// Create infrastructure managers
	_, chManager := createInfraManagers(log)

	// Cleanup test databases if requested
	if infraCleanupTestDBs {
		fmt.Println("Cleaning up ephemeral test databases...")
		if err := chManager.CleanupEphemeralDatabases(ctx, 0); err != nil {
			log.WithError(err).Warn("failed to cleanup test databases")
		}
	}

	// Stop infrastructure
	if err := chManager.Stop(); err != nil {
		return fmt.Errorf("stopping infrastructure: %w", err)
	}

	fmt.Println("\n✓ Platform infrastructure stopped successfully")

	return nil
}

func runInfraStatus(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup logger
	log := newLogger(infraVerbose)

	// Create infrastructure managers
	dockerManager, chManager := createInfraManagers(log)

	// Check if running
	running, err := dockerManager.IsRunning(ctx)
	if err != nil {
		return fmt.Errorf("checking if running: %w", err)
	}

	if !running {
		fmt.Println("Platform Status: STOPPED")
		return nil
	}

	fmt.Println("Platform Status: RUNNING")

	// Health check
	if err := chManager.HealthCheck(ctx); err != nil {
		fmt.Printf("ClickHouse Health: UNHEALTHY (%v)\n", err)
		return nil
	}

	fmt.Println("ClickHouse Health: HEALTHY")

	// Get container status if verbose
	if infraVerbose {
		fmt.Println("\nContainer Status:")
		status, err := dockerManager.GetContainerStatus(ctx, config.ClickHouseContainer)
		if err == nil {
			fmt.Printf("  ClickHouse: %s\n", status)
		}
	}

	return nil
}

func runInfraReset(cmd *cobra.Command, args []string) error {
	// Setup logger
	log := newLogger(false)

	fmt.Println("Resetting platform infrastructure (removing all volumes)...")

	// Create infrastructure managers
	dockerManager, _ := createInfraManagers(log)

	// Reset infrastructure
	if err := dockerManager.Reset(); err != nil {
		return fmt.Errorf("resetting infrastructure: %w", err)
	}

	fmt.Println("\n✓ Platform infrastructure reset successfully")
	fmt.Println("\nAll volumes removed. Run 'xatu-cbt infra start' to begin fresh.")

	return nil
}
