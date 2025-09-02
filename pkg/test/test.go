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
	ErrTestDirNotExist     = errors.New("test directory does not exist")
	ErrCBTEngineNotRunning = errors.New("CBT engine is not running")
	ErrAssertionTimeout    = errors.New("timeout reached while running assertions")
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

	testDir := filepath.Join(s.cfg.TestsDir, testName)
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

		if err := s.setupXatu(ctx, testName); err != nil {
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

	return s.runAssertions(ctx, testName)
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

func (s *service) setupCBT(ctx context.Context) error {
	s.log.Info("Setting up CBT environment")

	// 1. Stop cbt-engine container (keep redis running for faster restart)
	s.log.Debug("Stopping cbt-engine container")
	stopCmd := exec.Command("docker", "stop", "cbt-engine")
	if err := stopCmd.Run(); err != nil {
		s.log.WithError(err).Debug("Failed to stop cbt-engine (might not be running)")
	}

	// 2. Remove the old cbt-config-setup container if it exists
	removeCmd := exec.Command("docker", "rm", "-f", "cbt-config-setup")
	if err := removeCmd.Run(); err != nil {
		s.log.WithError(err).Debug("Failed to remove cbt-config-setup (might not exist)")
	}

	// 3. Run network teardown to clean up any existing network database
	// This ensures we have a clean state and avoids replica already exists errors
	s.log.Debug("Running network teardown to ensure clean state")
	if err := s.docker.RunCommand(ctx, ".", "./bin/xatu-cbt", "network", "teardown", "--force"); err != nil {
		// This might fail if network doesn't exist, which is ok
		s.log.WithError(err).Debug("Network teardown failed (might not exist)")
	}

	// 4. Run network setup to create the network database
	s.log.Debug("Running network setup")
	if err := s.docker.RunCommand(ctx, ".", "./bin/xatu-cbt", "network", "setup", "--force"); err != nil { //nolint:nestif // Retry logic for handling replica conflicts
		// If setup fails due to replica already exists, try teardown and setup again
		if strings.Contains(err.Error(), "REPLICA_ALREADY_EXISTS") {
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
		} else {
			return fmt.Errorf("failed to run network setup: %w", err)
		}
	}

	// 5. Start/restart only the necessary containers
	s.log.Debug("Starting CBT containers")

	// First, ensure redis is running (it should already be running)
	redisCmd := exec.Command("docker", "compose", "up", "-d", "cbt-redis")
	redisCmd.Dir = "."
	redisCmd.Env = os.Environ()
	if err := redisCmd.Run(); err != nil {
		s.log.WithError(err).Debug("Failed to ensure redis is running")
	}

	// Create the config by running cbt-config-setup
	configCmd := exec.Command("docker", "compose", "up", "-d", "cbt-config-setup")
	configCmd.Dir = "."
	configCmd.Env = os.Environ()
	if err := configCmd.Run(); err != nil {
		return fmt.Errorf("failed to create config: %w", err)
	}

	// Wait for config creation to complete
	time.Sleep(2 * time.Second)

	// Start cbt-engine
	engineCmd := exec.Command("docker", "compose", "up", "-d", "--no-recreate", "cbt-engine")
	engineCmd.Dir = "."
	engineCmd.Env = os.Environ()
	if err := engineCmd.Run(); err != nil {
		return fmt.Errorf("failed to start cbt-engine: %w", err)
	}

	// Ensure the container actually starts (sometimes it's just created)
	startCmd := exec.Command("docker", "start", "cbt-engine")
	if err := startCmd.Run(); err != nil {
		s.log.WithError(err).Debug("Failed to ensure cbt-engine is started")
	}

	// 5. Wait for CBT engine to start
	// The cbt-engine container uses a distroless image without health check support
	// We'll wait a bit for it to start and then verify it's running
	s.log.Info("Waiting for CBT engine to start...")
	time.Sleep(5 * time.Second)

	// Verify the container is running
	cmd := exec.Command("docker", "ps", "--filter", "name=cbt-engine", "--filter", "status=running", "--format", "{{.Names}}")
	output, err := cmd.Output()
	if err != nil || strings.TrimSpace(string(output)) != "cbt-engine" {
		// Get logs for debugging
		logsCmd := exec.Command("docker", "logs", "cbt-engine", "--tail", "20")
		if logsOutput, logErr := logsCmd.Output(); logErr == nil {
			s.log.WithField("logs", string(logsOutput)).Error("CBT engine logs")
		}
		return ErrCBTEngineNotRunning
	}

	s.log.Info("CBT engine started successfully")

	return nil
}

func (s *service) setupXatu(ctx context.Context, testName string) error {
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

	// Set default ClickHouse password if not set (migrator needs this)
	if os.Getenv("CLICKHOUSE_PASSWORD") == "" {
		s.log.Info("Setting CLICKHOUSE_PASSWORD to 'supersecret' for migrator")
		_ = os.Setenv("CLICKHOUSE_PASSWORD", "supersecret")
	}

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
	s.log.Debug("Ingesting test data")
	dataDir := filepath.Join(s.cfg.TestsDir, testName, "data")
	if err := s.dataLoader.LoadTestData(ctx, dataDir); err != nil {
		return fmt.Errorf("failed to load test data: %w", err)
	}

	return nil
}

func (s *service) runAssertions(ctx context.Context, testName string) error {
	s.log.Info("Running assertions phase")

	assertionsDir := filepath.Join(s.cfg.TestsDir, testName, "assertions")

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
