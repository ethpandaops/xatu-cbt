package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/infra"
	"github.com/ethpandaops/xatu-cbt/internal/testing"
	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	xatuModeLocal    = "local"
	xatuModeExternal = "external"
)

var (
	infraCleanupTestDBs bool
	infraVerbose        bool
	infraClickhouseURL  string
	infraRedisURL       string

	errInvalidXatuMode         = errors.New("invalid xatu-mode")
	errXatuRepositoryNotFound  = errors.New("xatu repository not found")
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
	infraCmd.AddCommand(infraStartCmd)
	infraCmd.AddCommand(infraStopCmd)
	infraCmd.AddCommand(infraStatusCmd)
	infraCmd.AddCommand(infraResetCmd)
	infraCmd.PersistentFlags().StringVar(&infraClickhouseURL, "clickhouse-url", config.GetCBTClickHouseURL(), "CBT ClickHouse cluster URL")
	infraCmd.PersistentFlags().StringVar(&infraRedisURL, "redis-url", config.DefaultRedisURL, "Redis connection URL")
	infraStopCmd.Flags().BoolVar(&infraCleanupTestDBs, "cleanup-test-dbs", true, "Cleanup ephemeral test databases")
	infraStatusCmd.Flags().BoolVar(&infraVerbose, "verbose", false, "Show detailed container and database information")
	infraStartCmd.Flags().String("xatu-source", xatuModeLocal, "Xatu data source: 'local' (start local cluster) or 'external' (connect to remote)")
	infraStartCmd.Flags().String("xatu-url", "", "External Xatu ClickHouse URL (required for --xatu-source=external). Format: [http|https://][username:password@]host:port")
}

// createInfraManagers creates and returns Docker and ClickHouse managers with the shared configuration.
func createInfraManagers(log logrus.FieldLogger) (infra.DockerManager, infra.ClickHouseManager) {
	dockerManager := infra.NewDockerManager(
		log,
		config.PlatformComposeFile,
		config.ProjectName,
	)

	// Load config to get safe hostnames for infrastructure management
	cfg, err := config.Load()
	if err != nil {
		log.WithError(err).Warn("failed to load config, using empty safe hostnames list")
		cfg = &config.AppConfig{SafeHostnames: []string{}}
	}

	chManager := infra.NewClickHouseManager(
		log,
		dockerManager,
		infraClickhouseURL,
		cfg.SafeHostnames,
	)

	return dockerManager, chManager
}

// ensureXatuRepo ensures the xatu repository exists and returns its path.
func ensureXatuRepo(log logrus.FieldLogger, wd, repoURL, ref string) (string, error) {
	xatuRepoManager := testing.NewRepoManager(log, wd, repoURL, ref)
	repoPath, err := xatuRepoManager.EnsureRepo()
	if err != nil {
		return "", fmt.Errorf("ensuring xatu repository: %w", err)
	}
	return repoPath, nil
}

func runInfraStart(cmd *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log := newLogger(false)
	log.Info("starting platform infrastructure")

	// Get xatu-source flag
	xatuSource, err := cmd.Flags().GetString("xatu-source")
	if err != nil {
		return fmt.Errorf("reading xatu-source flag: %w", err)
	}

	// Validate source
	if xatuSource != xatuModeLocal && xatuSource != xatuModeExternal {
		return fmt.Errorf("%w: %s (must be 'local' or 'external')", errInvalidXatuMode, xatuSource)
	}

	log.WithField("xatu_source", xatuSource).Info("configured Xatu source")

	// Set ClickHouse config directory based on source
	if xatuSource == xatuModeExternal {
		// Get external Xatu URL
		xatuURL, _ := cmd.Flags().GetString("xatu-url")
		if xatuURL == "" {
			return fmt.Errorf("--xatu-url is required when --xatu-source=external")
		}

		// Generate ClickHouse config with external cluster settings from URL
		if genErr := infra.GenerateExternalClickHouseConfigFromURL(log, xatuURL); genErr != nil {
			return fmt.Errorf("generating external ClickHouse config: %w", genErr)
		}

		if setenvErr := os.Setenv("CLICKHOUSE_CONFIG_DIR", "clickhouse-external"); setenvErr != nil {
			return fmt.Errorf("setting CLICKHOUSE_CONFIG_DIR: %w", setenvErr)
		}
		log.Debug("using external ClickHouse configuration")
	} else {
		if setenvErr := os.Setenv("CLICKHOUSE_CONFIG_DIR", "clickhouse"); setenvErr != nil {
			return fmt.Errorf("setting CLICKHOUSE_CONFIG_DIR: %w", setenvErr)
		}
		log.Debug("using local ClickHouse configuration")
	}

	// Check xatu repository if needed for local mode
	if xatuSource == xatuModeLocal {
		xatuRepo := filepath.Join(".", "xatu")
		log.WithField("path", xatuRepo).Debug("verifying xatu repository exists")

		if _, statErr := os.Stat(xatuRepo); os.IsNotExist(statErr) {
			return fmt.Errorf("%w at %s (required for local Xatu source)", errXatuRepositoryNotFound, xatuRepo)
		}
	}

	dockerManager, chManager := createInfraManagers(log)

	// Determine profiles to activate
	var profiles []string
	if xatuSource == xatuModeLocal {
		profiles = []string{"xatu-local"}
		log.Debug("activating xatu-local profile")
	} else {
		log.Info("skipping local Xatu cluster (external source)")
	}

	// Start infrastructure with profiles
	if startErr := chManager.Start(ctx, profiles...); startErr != nil {
		return fmt.Errorf("starting infrastructure: %w", startErr)
	}

	fmt.Println("\n✓ Platform infrastructure started successfully")
	if xatuSource == xatuModeExternal {
		fmt.Println("  Note: Local Xatu cluster NOT started (external source)")
	}

	services, err := dockerManager.GetAllServices(ctx)
	if err != nil {
		log.WithError(err).Debug("failed to get service information")
	} else if len(services) > 0 {
		fmt.Println()
		displayServicesTable(services)
	}

	fmt.Printf("\nConnection Information:\n")
	fmt.Printf("  CBT ClickHouse URL:  %s\n", infraClickhouseURL)
	fmt.Printf("  Xatu ClickHouse URL: %s\n", config.GetXatuClickHouseURL())
	fmt.Printf("  Redis URL:           %s\n", infraRedisURL)
	fmt.Printf("\nReady for testing and development!\n")
	fmt.Printf("\nRun 'xatu-cbt infra stop' to tear down, or 'xatu-cbt infra reset' to clear volumes.\n")

	return nil
}

// displayServicesTable renders a table of services with their status and ports.
func displayServicesTable(services []infra.ServiceInfo) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Service", "Type", "Status", "Ports"})
	table.SetBorder(true)
	table.SetRowLine(false)
	table.SetAutoWrapText(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)

	for _, svc := range services {
		var (
			serviceType = categorizeService(svc.Name)
			status      = svc.Status
			ports       = svc.Ports
		)

		if ports == "" {
			ports = "-"
		}

		table.Append([]string{svc.Name, serviceType, status, ports})
	}

	fmt.Println("Platform Services:")
	table.Render()
}

func runInfraStop(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log := newLogger(false)
	log.Info("stopping platform infrastructure")

	dockerManager, chManager := createInfraManagers(log)

	running, err := dockerManager.IsRunning(ctx)
	if err != nil {
		log.WithError(err).Debug("failed to check if running")
	}

	if running && infraCleanupTestDBs {
		log.Info("cleaning up ephemeral databases")
		if err := chManager.CleanupEphemeralDatabases(ctx, 0); err != nil {
			log.WithError(err).Warn("failed to cleanup test databases")
		}
	} else if !running {
		log.Debug("infrastructure not running, skipping database cleanup")
	}

	// Pass all known profiles to ensure complete shutdown
	// This ensures profiled containers (like xatu-clickhouse) are also stopped
	if err := chManager.Stop("xatu-local"); err != nil {
		return fmt.Errorf("stopping infrastructure: %w", err)
	}

	fmt.Println("\n✓ Platform infrastructure stopped successfully")
	fmt.Println("\nVolumes are preserved to maintain data.")
	fmt.Println("To remove all volumes and start fresh, run: xatu-cbt infra reset")

	return nil
}

func runInfraStatus(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log := newLogger(infraVerbose)

	dockerManager, chManager := createInfraManagers(log)

	running, err := dockerManager.IsRunning(ctx)
	if err != nil {
		return fmt.Errorf("checking if running: %w", err)
	}

	if !running {
		fmt.Println("Platform Status: STOPPED")
		return nil
	}

	fmt.Println("Platform Status: RUNNING")

	if err := chManager.HealthCheck(ctx); err != nil {
		fmt.Printf("ClickHouse Health: UNHEALTHY (%v)\n", err)
		return nil
	}

	fmt.Println("ClickHouse Health: HEALTHY")

	if infraVerbose {
		fmt.Println("\nContainer Status:")
		status, err := dockerManager.GetContainerStatus(ctx, config.ClickHouseContainer)
		if err == nil {
			fmt.Printf("  ClickHouse: %s\n", status)
		}
	}

	return nil
}

func runInfraReset(_ *cobra.Command, _ []string) error {
	log := newLogger(false)

	log.Info("Resetting platform infrastructure")

	dockerManager, _ := createInfraManagers(log)

	// Pass all known profiles to ensure complete cleanup
	// This ensures profiled containers (like xatu-clickhouse) are also removed
	if err := dockerManager.Reset("xatu-local"); err != nil {
		return fmt.Errorf("resetting infrastructure: %w", err)
	}

	fmt.Println("\n✓ Platform infrastructure reset successfully")
	fmt.Println("\nAll volumes removed. Run 'xatu-cbt infra start' to begin fresh.")

	return nil
}

// categorizeService returns the type/category of a service based on its name.
func categorizeService(name string) string {
	switch {
	case strings.HasPrefix(name, "xatu-cbt-clickhouse-zookeeper"):
		return "Coordination"
	case strings.HasPrefix(name, "xatu-cbt-clickhouse"):
		return "CBT Cluster"
	case strings.HasPrefix(name, "xatu-clickhouse"):
		return "Xatu Cluster"
	case strings.HasPrefix(name, "xatu-cbt-redis"):
		return "State Management"
	default:
		return "Other"
	}
}
