package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// Common errors
var (
	ErrTestDirNotExist      = errors.New("test directory does not exist")
	ErrCBTEngineNotRunning  = errors.New("CBT engine is not running")
	ErrAssertionTimeout     = errors.New("timeout reached while running assertions")
	ErrNetworkNotSet        = errors.New("NETWORK environment variable is not set")
	ErrInvalidTestNameFormat = errors.New("test name must be in network/spec format (e.g., mainnet/pectra)")
)

// Service provides test orchestration functionality
type Service interface {
	Start(ctx context.Context) error
	Stop() error
	RunTest(ctx context.Context, testName string, skipSetup bool) error
	Teardown(ctx context.Context) error
}

type service struct {
	log             logrus.FieldLogger
	cfg             Config
	dataLoader      DataLoader
	assertionRunner AssertionRunner
	docker          DockerManager
}

// Config holds the test service configuration
type Config struct {
	TestsDir      string
	XatuRef       string
	XatuRepoURL   string
	Timeout       time.Duration
	CheckInterval time.Duration
}

// NewService creates a new test service instance
func NewService(log logrus.FieldLogger, cfg Config) Service {
	return &service{
		log:             log.WithField("service", "test"),
		cfg:             cfg,
		dataLoader:      NewDataLoader(log),
		assertionRunner: NewAssertionRunner(log),
		docker:          NewDockerManager(log),
	}
}

func (s *service) Start(_ context.Context) error {
	s.log.Info("Starting test service")
	return nil
}

func (s *service) Stop() error {
	s.log.Info("Stopping test service")
	return nil
}

func (s *service) RunTest(ctx context.Context, testName string, skipSetup bool) error {
	s.log.WithField("test", testName).Info("Running test")

	// Extract network from testName (format: network/spec)
	network, err := s.extractNetworkFromTestName(testName)
	if err != nil {
		return fmt.Errorf("failed to extract network from test name: %w", err)
	}

	// Set NETWORK environment variable for all downstream operations
	// This ensures config, docker-compose, and CBT all use the correct network
	s.log.WithField("network", network).Info("Setting NETWORK environment variable from test path")
	if err := os.Setenv("NETWORK", network); err != nil {
		return fmt.Errorf("failed to set NETWORK environment variable: %w", err)
	}

	// Resolve test paths from network/spec format (e.g., mainnet/pectra, sepolia/fusaka)
	testDir, dataDir, assertionsDir := s.resolveTestPaths(testName)

	// Check if test directory exists
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		return fmt.Errorf("%w: %s", ErrTestDirNotExist, testDir)
	}

	// Phase 1: Setup Xatu (can be skipped if already exists)
	xatuExists := s.checkXatuExists(ctx)
	if xatuExists && skipSetup { //nolint:nestif // Clear logic for handling different setup scenarios
		s.log.Info("Xatu environment detected, skipping xatu setup")
	} else {
		if xatuExists && !skipSetup {
			s.log.Info("Xatu environment detected but not skipping setup, will recreate")
		} else if !xatuExists && skipSetup {
			s.log.Warn("Xatu environment not detected, cannot skip setup - will run full setup")
		}

		if err := s.setupXatu(ctx, dataDir); err != nil {
			return fmt.Errorf("xatu setup failed: %w", err)
		}
	}

	// Phase 2: Setup CBT (always runs)
	s.log.Info("Setting up CBT environment")
	if err := s.setupCBT(ctx); err != nil {
		return fmt.Errorf("CBT setup failed: %w", err)
	}

	// Phase 3: Run assertions with timeout
	s.log.Info("Starting assertion phase")
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	return s.runAssertions(ctx, assertionsDir)
}

func (s *service) Teardown(ctx context.Context) error {
	s.log.Info("Running teardown")

	// Stop local docker compose
	if err := s.docker.ComposeDown(ctx, ".", true); err != nil {
		s.log.WithError(err).Warn("Failed to stop local docker compose")
	}

	// Clean up xatu directory if it exists
	xatuDir := "xatu"
	if _, err := os.Stat(xatuDir); err == nil {
		// Stop xatu docker compose
		if err := s.docker.ComposeDown(ctx, xatuDir, true); err != nil {
			s.log.WithError(err).Warn("Failed to stop xatu docker compose")
		}

		// Remove xatu directory
		if err := os.RemoveAll(xatuDir); err != nil {
			s.log.WithError(err).Warn("Failed to remove xatu directory")
		}
	}

	return nil
}

// extractNetworkFromTestName extracts the network name from the test name
// Format: network/spec (e.g., "mainnet/pectra" -> "mainnet", "sepolia/fusaka" -> "sepolia")
func (s *service) extractNetworkFromTestName(testName string) (string, error) {
	parts := strings.Split(testName, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("%w, got: %s", ErrInvalidTestNameFormat, testName)
	}
	return parts[0], nil
}

// resolveTestPaths resolves test directory structure
// Format: tests/network/spec (e.g., tests/mainnet/pectra, tests/sepolia/fusaka)
func (s *service) resolveTestPaths(testName string) (testDir, dataDir, assertionsDir string) {
	// testName must be in network/spec format (e.g., "mainnet/pectra", "sepolia/fusaka")
	testDir = filepath.Join(s.cfg.TestsDir, testName)
	dataDir = filepath.Join(testDir, "data")
	assertionsDir = filepath.Join(testDir, "assertions")
	return testDir, dataDir, assertionsDir
}

func (s *service) checkXatuExists(_ context.Context) bool {
	// Check if xatu directory exists
	if _, err := os.Stat("xatu"); os.IsNotExist(err) {
		return false
	}

	// Check if xatu-clickhouse containers are running
	cmd := exec.Command("docker", "ps", "--filter", "name=xatu-clickhouse", "--format", "{{.Names}}")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	// If we have xatu-clickhouse containers running, xatu is set up
	return strings.TrimSpace(string(output)) != ""
}

// getNetworkName returns the network name from environment variable
func (s *service) getNetworkName() (string, error) {
	network := os.Getenv("NETWORK")
	if network == "" {
		return "", ErrNetworkNotSet
	}
	return network, nil
}

func (s *service) setupCBT(ctx context.Context) error {
	s.log.Info("Setting up CBT environment")

	// Get network name for container suffixes
	network, err := s.getNetworkName()
	if err != nil {
		return fmt.Errorf("failed to get network name: %w", err)
	}
	cbtEngine := fmt.Sprintf("cbt-engine-%s", network)
	cbtConfigSetup := fmt.Sprintf("cbt-config-setup-%s", network)

	// 1. Stop cbt-engine container (keep redis running for faster restart)
	s.log.WithField("container", cbtEngine).Debug("Stopping cbt-engine container")
	stopCmd := exec.Command("docker", "stop", cbtEngine) //nolint:gosec // cbtEngine is controlled input from env var
	if stopErr := stopCmd.Run(); stopErr != nil {
		s.log.WithError(stopErr).Debug("Failed to stop cbt-engine (might not be running)")
	}

	// 2. Remove the old cbt-config-setup container if it exists
	removeCmd := exec.Command("docker", "rm", "-f", cbtConfigSetup) //nolint:gosec // cbtConfigSetup is controlled input from env var
	if removeErr := removeCmd.Run(); removeErr != nil {
		s.log.WithError(removeErr).Debug("Failed to remove cbt-config-setup (might not exist)")
	}

	// 3. Run network teardown to clean up any existing network database
	// This ensures we have a clean state and avoids replica already exists errors
	s.log.Debug("Running network teardown to ensure clean state")
	if teardownErr := s.docker.RunCommand(ctx, ".", "./bin/xatu-cbt", "network", "teardown", "--force"); teardownErr != nil {
		// This might fail if network doesn't exist, which is ok
		s.log.WithError(teardownErr).Debug("Network teardown failed (might not exist)")
	}

	// 4. Run network setup to create the network database
	if err := s.setupCBTNetwork(ctx); err != nil {
		return err
	}

	// 5. Start/restart only the necessary containers
	if err := s.startCBTContainers(ctx, network, cbtEngine); err != nil {
		return err
	}

	// 6. Wait for CBT engine to start
	if err := s.waitForCBTEngine(cbtEngine); err != nil {
		return err
	}

	s.log.Info("CBT engine started successfully")

	return nil
}

func (s *service) setupCBTNetwork(ctx context.Context) error {
	s.log.Debug("Running network setup")
	setupErr := s.docker.RunCommand(ctx, ".", "./bin/xatu-cbt", "network", "setup", "--force")
	if setupErr == nil {
		return nil
	}

	// If setup fails due to replica already exists, try teardown and setup again
	if !strings.Contains(setupErr.Error(), "REPLICA_ALREADY_EXISTS") {
		return fmt.Errorf("failed to run network setup: %w", setupErr)
	}

	s.log.Warn("Replica already exists, attempting cleanup and retry")

	// Force teardown to clean up existing replicas
	if teardownErr := s.docker.RunCommand(ctx, ".", "./bin/xatu-cbt", "network", "teardown", "--force"); teardownErr != nil {
		s.log.WithError(teardownErr).Warn("Failed to teardown during retry")
	}

	// Wait a moment for cleanup to complete
	time.Sleep(2 * time.Second)

	// Try setup again
	if retryErr := s.docker.RunCommand(ctx, ".", "./bin/xatu-cbt", "network", "setup", "--force"); retryErr != nil {
		return fmt.Errorf("failed to run network setup after retry: %w", retryErr)
	}

	return nil
}

func (s *service) startCBTContainers(_ context.Context, network, cbtEngine string) error {
	s.log.Debug("Starting CBT containers")

	// Stop cbt-engine first to ensure it will reload the new config
	s.log.WithField("container", cbtEngine).Debug("Stopping cbt-engine container")
	stopEngineCmd := exec.Command("docker", "stop", cbtEngine) //nolint:gosec // cbtEngine is controlled input from env var
	if stopErr := stopEngineCmd.Run(); stopErr != nil {
		s.log.WithError(stopErr).Debug("cbt-engine might not be running")
	} else {
		s.log.WithField("container", cbtEngine).Debug("cbt-engine stopped successfully")
	}

	// Remove the old config container to ensure a fresh one is created
	configContainerName := fmt.Sprintf("cbt-config-setup-%s", network)
	s.log.WithField("container", configContainerName).Debug("Removing old config container")
	rmConfigCmd := exec.Command("docker", "rm", "-f", configContainerName) //nolint:gosec // network is controlled input from env var
	if rmErr := rmConfigCmd.Run(); rmErr != nil {
		s.log.WithError(rmErr).Debug("Config container might not exist")
	} else {
		s.log.WithField("container", configContainerName).Debug("Config container removed")
	}

	// First, ensure redis is running (it should already be running)
	redisContainerName := fmt.Sprintf("cbt-redis-%s", network)
	s.log.WithField("container", redisContainerName).Debug("Ensuring redis is running")
	redisCmd := exec.Command("docker", "compose", "up", "-d", "cbt-redis")
	redisCmd.Dir = "."
	redisCmd.Env = os.Environ()
	redisCmd.Env = append(redisCmd.Env, "TESTING=true")
	if redisErr := redisCmd.Run(); redisErr != nil {
		s.log.WithError(redisErr).Debug("Failed to ensure redis is running")
	} else {
		s.log.WithField("container", redisContainerName).Debug("Redis container is running")
	}

	// Force recreate the config by running cbt-config-setup
	// This ensures the config is regenerated with the latest overrides
	// Use 'run' instead of 'up' to force execution even if container already ran
	s.log.WithField("container", configContainerName).Debug("Creating new config with TESTING=true")
	configCmd := exec.Command("docker", "compose", "run", "--rm", "cbt-config-setup")
	configCmd.Dir = "."
	configCmd.Env = os.Environ()
	configCmd.Env = append(configCmd.Env, "TESTING=true")
	if configErr := configCmd.Run(); configErr != nil {
		return fmt.Errorf("failed to create config: %w", configErr)
	}
	s.log.WithField("container", configContainerName).Debug("Config created successfully with test overrides")

	// Wait for config creation to complete
	time.Sleep(2 * time.Second)

	// Force recreate cbt-engine to ensure it picks up the new config
	s.log.WithField("container", cbtEngine).Debug("Starting cbt-engine with new config")
	engineCmd := exec.Command("docker", "compose", "up", "-d", "--force-recreate", "cbt-engine")
	engineCmd.Dir = "."
	engineCmd.Env = os.Environ()
	engineCmd.Env = append(engineCmd.Env, "TESTING=true")
	if engineErr := engineCmd.Run(); engineErr != nil {
		return fmt.Errorf("failed to start cbt-engine: %w", engineErr)
	}
	s.log.WithField("container", cbtEngine).Debug("cbt-engine started with force-recreate")

	// Ensure the container actually starts (sometimes it's just created)
	startCmd := exec.Command("docker", "start", cbtEngine) //nolint:gosec // cbtEngine is controlled input from env var
	if startErr := startCmd.Run(); startErr != nil {
		s.log.WithError(startErr).Debug("Failed to ensure cbt-engine is started")
	}

	return nil
}

func (s *service) waitForCBTEngine(cbtEngine string) error {
	// The cbt-engine container uses a distroless image without health check support
	// We'll wait a bit for it to start and then verify it's running
	s.log.Info("Waiting for CBT engine to start...")
	time.Sleep(5 * time.Second)

	// Verify the container is running
	cmd := exec.Command("docker", "ps", "--filter", fmt.Sprintf("name=%s", cbtEngine), "--filter", "status=running", "--format", "{{.Names}}") //nolint:gosec // cbtEngine is controlled input from env var
	output, err := cmd.Output()
	if err != nil || strings.TrimSpace(string(output)) != cbtEngine {
		// Get logs for debugging
		logsCmd := exec.Command("docker", "logs", cbtEngine, "--tail", "20") //nolint:gosec // cbtEngine is controlled input from env var
		if logsOutput, logErr := logsCmd.Output(); logErr == nil {
			s.log.WithField("logs", string(logsOutput)).Error("CBT engine logs")
		}
		return ErrCBTEngineNotRunning
	}

	return nil
}

func (s *service) setupXatu(ctx context.Context, dataDir string) error {
	s.log.Info("Setting up Xatu environment")

	xatuRef := s.cfg.XatuRef
	if xatuRef == "" {
		xatuRef = os.Getenv("XATU_REF")
		if xatuRef == "" {
			xatuRef = "master"
		}
	}

	s.log.WithField("ref", xatuRef).Debug("Cloning xatu repository")
	if err := s.docker.CloneRepository(ctx, s.cfg.XatuRepoURL, "xatu", xatuRef); err != nil {
		return fmt.Errorf("failed to clone xatu repository: %w", err)
	}

	// 2. Start xatu docker compose with clickhouse profile
	s.log.Debug("Starting xatu docker compose")
	if err := s.docker.ComposeDown(ctx, "xatu", true); err != nil {
		return fmt.Errorf("failed to stop xatu docker compose: %w", err)
	}
	if err := s.docker.ComposeUp(ctx, "xatu", []string{"clickhouse"}); err != nil {
		return fmt.Errorf("failed to start xatu docker compose: %w", err)
	}

	// 3. Wait for xatu-clickhouse-migrator to finish
	s.log.Info("Waiting for xatu ClickHouse migrations to complete (this may take a few minutes)...")
	if err := s.docker.WaitForContainerExit(ctx, "xatu-clickhouse-migrator", 60*time.Minute); err != nil {
		return fmt.Errorf("failed waiting for migrations: %w", err)
	}
	s.log.Info("ClickHouse migrations completed successfully")

	// 4. Ingest test data into ClickHouse
	s.log.WithField("data_dir", dataDir).Debug("Ingesting test data")
	if err := s.dataLoader.LoadTestData(ctx, dataDir); err != nil {
		return fmt.Errorf("failed to load test data: %w", err)
	}

	// 5. Start local CBT ClickHouse containers
	s.log.Debug("Starting local CBT ClickHouse containers")
	if err := s.docker.ComposeUp(ctx, ".", []string{"clickhouse"}); err != nil {
		return fmt.Errorf("failed to start CBT ClickHouse: %w", err)
	}

	// 6. Wait for CBT ClickHouse containers to be healthy
	s.log.Info("Waiting for CBT ClickHouse containers to be healthy...")
	if err := s.docker.WaitForContainerHealthy(ctx, "xatu-cbt-clickhouse-01", 5*time.Minute); err != nil {
		return fmt.Errorf("failed waiting for xatu-cbt-clickhouse-01: %w", err)
	}
	if err := s.docker.WaitForContainerHealthy(ctx, "xatu-cbt-clickhouse-02", 5*time.Minute); err != nil {
		return fmt.Errorf("failed waiting for xatu-cbt-clickhouse-02: %w", err)
	}
	s.log.Info("CBT ClickHouse containers are healthy")

	return nil
}

func (s *service) runAssertions(ctx context.Context, assertionsDir string) error {
	s.log.WithField("assertions_dir", assertionsDir).Info("Running assertions phase")

	ticker := time.NewTicker(s.cfg.CheckInterval)
	defer ticker.Stop()

	// Ticker for showing summary every 5 seconds
	summaryTicker := time.NewTicker(5 * time.Second)
	defer summaryTicker.Stop()

	startTime := time.Now()
	checkCount := 0

	// Run assertions immediately on start
	checkCount++
	s.log.Info("Running initial assertion check...")
	allPassed, err := s.assertionRunner.RunAssertions(ctx, assertionsDir)
	if err != nil {
		s.log.WithError(err).Warn("Error running assertions")
	} else if allPassed {
		s.log.Info("✅ All assertions passed successfully!")
		return nil
	}

	// Continue checking on interval
	for {
		select {
		case <-ctx.Done():
			return ErrAssertionTimeout

		case <-summaryTicker.C:
			// Show periodic progress update
			s.log.WithFields(logrus.Fields{
				"elapsed": time.Since(startTime).Round(time.Second),
				"checks":  checkCount,
			}).Info("Still checking assertions...")

		case <-ticker.C:
			checkCount++
			allPassed, err := s.assertionRunner.RunAssertions(ctx, assertionsDir)
			if err != nil {
				s.log.WithError(err).Warn("Error running assertions")
				continue
			}

			if allPassed {
				s.log.Info("✅ All assertions passed successfully!")
				return nil
			}
		}
	}
}
