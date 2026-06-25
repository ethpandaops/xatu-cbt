package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver
	"github.com/ethpandaops/xatu-cbt/internal/actions"
	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/infra"
	"github.com/ethpandaops/xatu-cbt/internal/testing"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/source/file" // file source driver
	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	xatuModeLocal    = "local"
	xatuModeExternal = "external"

	xatuClickHouseService = "xatu-clickhouse-01"
)

var (
	infraCleanupTestDBs bool
	infraVerbose        bool
	infraClickhouseURL  string
	infraRedisURL       string
	infraRunMigrations  bool
	infraXatuRef        string
	infraProjectName    string

	errInvalidXatuMode  = errors.New("invalid xatu-mode")
	errXatuURLRequired  = errors.New("xatu-url is required when xatu-source is external")
	errProjectNameEmpty = errors.New("project-name cannot be empty")
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

var infraMigrateXatuCmd = &cobra.Command{
	Use:   "migrate-xatu",
	Short: "Run xatu schema migrations against the local xatu ClickHouse cluster",
	Long: `Runs ONLY the xatu (external source) ClickHouse migrations against the local
xatu cluster. Unlike "infra start --run-migrations", this does NOT run CBT
migrations — it is the dedicated seam for orchestrators (e.g. xcli) that drive
CBT setup separately, per-network.

The xatu repository is cloned/updated as needed (use --xatu-ref to pin a ref).

Example:
  xatu-cbt infra migrate-xatu --xatu-ref master`,
	RunE: runInfraMigrateXatu,
}

func init() {
	infraCmd.AddCommand(infraStartCmd)
	infraCmd.AddCommand(infraStopCmd)
	infraCmd.AddCommand(infraStatusCmd)
	infraCmd.AddCommand(infraResetCmd)
	infraCmd.AddCommand(infraMigrateXatuCmd)
	infraMigrateXatuCmd.Flags().StringVar(&infraXatuRef, "xatu-ref", config.XatuDefaultRef, "Xatu repository ref (branch/tag/commit)")
	infraCmd.PersistentFlags().StringVar(&infraProjectName, "project-name", config.GetProjectName(), fmt.Sprintf("Docker Compose project name (env %s)", config.ProjectNameEnvVar))
	infraCmd.PersistentFlags().StringVar(&infraClickhouseURL, "clickhouse-url", config.GetCBTClickHouseURL(), "CBT ClickHouse cluster URL")
	infraCmd.PersistentFlags().StringVar(&infraRedisURL, "redis-url", config.GetRedisURL(), "Redis connection URL")
	infraStopCmd.Flags().BoolVar(&infraCleanupTestDBs, "cleanup-test-dbs", true, "Cleanup ephemeral test databases")
	infraStatusCmd.Flags().BoolVar(&infraVerbose, "verbose", false, "Show detailed container and database information")
	infraStartCmd.Flags().String("xatu-source", xatuModeLocal, "Xatu data source: 'local' (start local cluster) or 'external' (connect to remote)")
	infraStartCmd.Flags().String("xatu-url", "", "External Xatu ClickHouse URL (required for --xatu-source=external). Format: [http|https://][username:password@]host:port")
	infraStartCmd.Flags().BoolVar(&infraRunMigrations, "run-migrations", false, "Run migrations for both Xatu and CBT clusters after starting infrastructure")
	infraStartCmd.Flags().StringVar(&infraXatuRef, "xatu-ref", config.XatuDefaultRef, "Xatu repository ref (branch/tag/commit)")
}

func resolveInfraProjectName() (string, error) {
	projectName := strings.TrimSpace(infraProjectName)
	if projectName == "" {
		return "", errProjectNameEmpty
	}

	return projectName, nil
}

// createInfraManagers creates and returns Docker and ClickHouse managers with the shared configuration.
func createInfraManagers(log logrus.FieldLogger) (infra.DockerManager, infra.ClickHouseManager, error) {
	projectName, err := resolveInfraProjectName()
	if err != nil {
		return nil, nil, err
	}

	dockerManager := infra.NewDockerManager(
		log,
		config.PlatformComposeFile,
		projectName,
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

	return dockerManager, chManager, nil
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

// configureXatuSource configures the ClickHouse config directory based on the xatu source mode.
func configureXatuSource(log logrus.FieldLogger, xatuSource, xatuURL string) ([]string, error) {
	if xatuSource == xatuModeExternal {
		if xatuURL == "" {
			return nil, errXatuURLRequired
		}

		// Generate ClickHouse config with external cluster settings from URL
		if genErr := infra.GenerateExternalClickHouseConfigFromURL(log, xatuURL); genErr != nil {
			return nil, fmt.Errorf("generating external ClickHouse config: %w", genErr)
		}

		if setenvErr := os.Setenv("CLICKHOUSE_CONFIG_DIR", "clickhouse-external"); setenvErr != nil {
			return nil, fmt.Errorf("setting CLICKHOUSE_CONFIG_DIR: %w", setenvErr)
		}

		log.Debug("using external ClickHouse configuration")
		return nil, nil // No profiles for external mode
	}

	// Local mode
	if setenvErr := os.Setenv("CLICKHOUSE_CONFIG_DIR", "clickhouse"); setenvErr != nil {
		return nil, fmt.Errorf("setting CLICKHOUSE_CONFIG_DIR: %w", setenvErr)
	}

	log.Debug("using local ClickHouse configuration")
	return []string{"xatu-local"}, nil
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

	// Get external Xatu URL if provided
	xatuURL, _ := cmd.Flags().GetString("xatu-url")

	// Configure ClickHouse config directory and determine profiles
	profiles, err := configureXatuSource(log, xatuSource, xatuURL)
	if err != nil {
		return err
	}

	// Ensure xatu repository for local mode
	if xatuSource == xatuModeLocal {
		wd, wdErr := os.Getwd()
		if wdErr != nil {
			return fmt.Errorf("getting working directory: %w", wdErr)
		}

		xatuRepoPath, repoErr := ensureXatuRepo(log, wd, config.XatuRepoURL, infraXatuRef)
		if repoErr != nil {
			return fmt.Errorf("ensuring xatu repository: %w", repoErr)
		}

		log.WithField("path", xatuRepoPath).Debug("xatu repository ready")
		log.Debug("activating xatu-local profile")
	} else {
		log.Info("skipping local Xatu cluster (external source)")
	}

	dockerManager, chManager, err := createInfraManagers(log)
	if err != nil {
		return err
	}

	// Start infrastructure with profiles
	if startErr := chManager.Start(ctx, profiles...); startErr != nil {
		return fmt.Errorf("starting infrastructure: %w", startErr)
	}

	fmt.Println("\n✓ Platform infrastructure started successfully")
	if xatuSource == xatuModeExternal {
		fmt.Println("  Note: Local Xatu cluster NOT started (external source)")
	}

	// Run migrations if requested
	if infraRunMigrations {
		if migErr := handleInfraMigrations(ctx, log, dockerManager, xatuSource); migErr != nil {
			return migErr
		}
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

	dockerManager, chManager, err := createInfraManagers(log)
	if err != nil {
		return err
	}

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

	dockerManager, chManager, err := createInfraManagers(log)
	if err != nil {
		return err
	}

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

	dockerManager, _, err := createInfraManagers(log)
	if err != nil {
		return err
	}

	// Pass all known profiles to ensure complete cleanup
	// This ensures profiled containers (like xatu-clickhouse) are also removed
	if err := dockerManager.Reset("xatu-local"); err != nil {
		return fmt.Errorf("resetting infrastructure: %w", err)
	}

	fmt.Println("\n✓ Platform infrastructure reset successfully")
	fmt.Println("\nAll volumes removed. Run 'xatu-cbt infra start' to begin fresh.")

	return nil
}

// handleInfraMigrations orchestrates migrations for infrastructure startup.
func handleInfraMigrations(ctx context.Context, log logrus.FieldLogger, dockerManager infra.DockerManager, xatuSource string) error {
	fmt.Println("\n🔄 Running migrations...")

	xatuRepoPath, repoErr := getXatuRepoPathForMigrations(log, xatuSource)
	if repoErr != nil {
		return repoErr
	}

	if migErr := runInfraMigrations(ctx, log, dockerManager, xatuSource, xatuRepoPath); migErr != nil {
		return fmt.Errorf("running migrations: %w", migErr)
	}

	fmt.Println("✅ Migrations completed successfully")

	return nil
}

// getXatuRepoPathForMigrations returns the xatu repo path if in local mode.
func getXatuRepoPathForMigrations(log logrus.FieldLogger, xatuSource string) (string, error) {
	if xatuSource != xatuModeLocal {
		return "", nil
	}

	wd, wdErr := os.Getwd()
	if wdErr != nil {
		return "", fmt.Errorf("getting working directory: %w", wdErr)
	}

	repoPath, repoErr := ensureXatuRepo(log, wd, config.XatuRepoURL, infraXatuRef)
	if repoErr != nil {
		return "", fmt.Errorf("ensuring xatu repository for migrations: %w", repoErr)
	}

	return repoPath, nil
}

// runInfraMigrations runs migrations for both Xatu and CBT clusters.
func runInfraMigrations(ctx context.Context, log logrus.FieldLogger, dockerManager infra.DockerManager, xatuSource, xatuRepoPath string) error {
	projectName, projectErr := resolveInfraProjectName()
	if projectErr != nil {
		return projectErr
	}

	// Run Xatu migrations (only for local mode)
	if xatuSource == xatuModeLocal && xatuRepoPath != "" {
		fmt.Println("  → Running Xatu cluster migrations...")

		xatuMigrationDir := fmt.Sprintf("%s/%s", xatuRepoPath, config.XatuMigrationsPath)
		xatuConnStr, connErr := resolveXatuMigrationConnStr(ctx, dockerManager, projectName)
		if connErr != nil {
			return connErr
		}

		if err := runXatuMigrations(ctx, log, xatuMigrationDir, xatuConnStr); err != nil {
			return fmt.Errorf("xatu migrations: %w", err)
		}

		fmt.Println("  ✓ Xatu cluster migrations complete")
	}

	// Run CBT migrations
	fmt.Println("  → Running CBT cluster migrations...")

	restoreEnv, targetErr := configureCBTMigrationTarget(ctx, dockerManager, projectName)
	if targetErr != nil {
		return targetErr
	}
	defer restoreEnv()

	if err := actions.Setup(false, true); err != nil {
		return fmt.Errorf("cbt migrations: %w", err)
	}

	fmt.Println("  ✓ CBT cluster migrations complete")

	return nil
}

// runInfraMigrateXatu runs ONLY the xatu migrations against the local xatu cluster.
// It is the dedicated, CBT-free seam used by external orchestrators (e.g. xcli) so
// they can drive CBT network setup separately.
func runInfraMigrateXatu(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log := newLogger(false)

	dockerManager, _, err := createInfraManagers(log)
	if err != nil {
		return err
	}

	projectName, err := resolveInfraProjectName()
	if err != nil {
		return err
	}

	wd, wdErr := os.Getwd()
	if wdErr != nil {
		return fmt.Errorf("getting working directory: %w", wdErr)
	}

	xatuRepoPath, repoErr := ensureXatuRepo(log, wd, config.XatuRepoURL, infraXatuRef)
	if repoErr != nil {
		return fmt.Errorf("ensuring xatu repository: %w", repoErr)
	}

	baseMigrationDir := filepath.Join(xatuRepoPath, config.XatuMigrationsPath)
	xatuConnStr, connErr := resolveXatuMigrationConnStr(ctx, dockerManager, projectName)
	if connErr != nil {
		return connErr
	}

	fmt.Println("→ Running Xatu cluster migrations...")

	if err := runXatuMigrations(ctx, log, baseMigrationDir, xatuConnStr); err != nil {
		return fmt.Errorf("xatu migrations: %w", err)
	}

	fmt.Println("✓ Xatu cluster migrations complete")

	return nil
}

func configureCBTMigrationTarget(ctx context.Context, dockerManager infra.DockerManager, projectName string) (func(), error) {
	published, err := dockerManager.GetServicePort(ctx, config.ClickHouseContainer, config.ClickHouseContainerNativePort)
	if err != nil {
		return nil, fmt.Errorf("resolving CBT migration target for project %q: %w", projectName, err)
	}

	return applyEnv(map[string]string{
		"CLICKHOUSE_HOST":                      published.Host,
		config.ClickHouseCBT01NativePortEnvVar: published.Port,
	}), nil
}

func resolveXatuMigrationConnStr(ctx context.Context, dockerManager infra.DockerManager, projectName string) (string, error) {
	published, err := dockerManager.GetServicePort(ctx, xatuClickHouseService, config.ClickHouseContainerNativePort, "xatu-local")
	if err != nil {
		return "", fmt.Errorf("resolving Xatu migration target for project %q: %w", projectName, err)
	}

	return clickHouseURLForPublishedPort(published), nil
}

func clickHouseURLForPublishedPort(published infra.PublishedPort) string {
	username := os.Getenv("CLICKHOUSE_USERNAME")
	if username == "" {
		username = "default"
	}

	password := os.Getenv("CLICKHOUSE_PASSWORD")
	if password == "" {
		password = "supersecret"
	}

	return fmt.Sprintf("clickhouse://%s:%s@%s:%s", username, password, published.Host, published.Port)
}

func applyEnv(overrides map[string]string) func() {
	previous := make(map[string]*string, len(overrides))

	for key, value := range overrides {
		if current, ok := os.LookupEnv(key); ok {
			currentCopy := current
			previous[key] = &currentCopy
		} else {
			previous[key] = nil
		}

		_ = os.Setenv(key, value)
	}

	return func() {
		for key, value := range previous {
			if value == nil {
				_ = os.Unsetenv(key)
				continue
			}

			_ = os.Setenv(key, *value)
		}
	}
}

// runXatuMigrations runs xatu's per-schema migration sets against the Xatu
// ClickHouse cluster. Each set is database-agnostic and applied to its target
// database, tracked in its own schema_migrations_<set> table. baseMigrationDir is
// the root migrations directory; each set lives in a subdirectory below it.
func runXatuMigrations(ctx context.Context, log logrus.FieldLogger, baseMigrationDir, xatuConnStr string) error {
	// Open connection to Xatu cluster
	conn, openErr := sql.Open("clickhouse", xatuConnStr)
	if openErr != nil {
		return fmt.Errorf("opening xatu clickhouse connection: %w", openErr)
	}
	defer func() { _ = conn.Close() }()

	if pingErr := conn.PingContext(ctx); pingErr != nil {
		return fmt.Errorf("pinging xatu clickhouse: %w", pingErr)
	}

	// Create required databases. golang-migrate cannot bootstrap the database it
	// connects to, so every target database must exist before migrations run.
	if dbErr := createXatuDatabases(ctx, conn); dbErr != nil {
		return dbErr
	}

	for _, set := range config.XatuMigrationSets() {
		setDir := filepath.Join(baseMigrationDir, set.Name)
		if _, statErr := os.Stat(setDir); statErr != nil {
			return fmt.Errorf("xatu migration set %q not found at %s (xatu ref may predate the per-set layout): %w",
				set.Name, setDir, statErr)
		}

		log.WithFields(logrus.Fields{
			"set":      set.Name,
			"database": set.Database,
		}).Debug("running xatu migrations")

		if err := applyXatuMigrationSet(log, xatuConnStr, setDir, set); err != nil {
			return err
		}
	}

	return nil
}

// applyXatuMigrationSet applies a single xatu migration set to its target database.
func applyXatuMigrationSet(log logrus.FieldLogger, connStr, setDir string, set config.XatuMigrationSet) error {
	m, migErr := migrate.New(
		fmt.Sprintf("file://%s", setDir),
		fmt.Sprintf(
			"%s?database=%s&x-multi-statement=true&x-cluster-name=%s&x-migrations-table=%s%s&x-migrations-table-engine=ReplicatedMergeTree",
			connStr, set.Database, config.XatuClusterName, config.SchemaMigrationsPrefix, set.Name,
		),
	)
	if migErr != nil {
		return fmt.Errorf("creating migration instance for set %q: %w", set.Name, migErr)
	}
	defer func() {
		if _, closeErr := m.Close(); closeErr != nil {
			log.WithError(closeErr).Debug("failed to close migration instance")
		}
	}()

	if upErr := m.Up(); upErr != nil && !errors.Is(upErr, migrate.ErrNoChange) {
		return fmt.Errorf("running migrations for set %q: %w", set.Name, upErr)
	}

	return nil
}

// createXatuDatabases creates the per-set target databases in the Xatu cluster.
// golang-migrate cannot bootstrap the database it connects to, so the targets
// must exist before migrations run.
func createXatuDatabases(ctx context.Context, conn *sql.DB) error {
	for _, set := range config.XatuMigrationSets() {
		createSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER %s", set.Database, config.XatuClusterName)

		if _, execErr := conn.ExecContext(ctx, createSQL); execErr != nil {
			return fmt.Errorf("creating %s database: %w", set.Database, execErr)
		}
	}

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
