package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/savid/xatu-cbt/cmd"
	"github.com/savid/xatu-cbt/pkg/actions"
	"github.com/savid/xatu-cbt/pkg/interactive"
	"github.com/savid/xatu-cbt/pkg/test"
)

func runInteractive() {
	fmt.Println("Xatu CBT - Interactive Mode")
	fmt.Println("===========================")
	fmt.Println()

	for {
		options := []interactive.MenuOption{
			{
				Name:        "🧪 Test Management",
				Description: "Run tests and manage test environments",
				Action:      showTestMenu,
			},
			{
				Name:        "🗄️  Network Management",
				Description: "Setup, teardown, and configure network databases",
				Action:      showNetworkMenu,
			},
			{
				Name:        "📋 Show Config",
				Description: "Display current environment configuration",
				Action: func() error {
					if err := actions.ShowConfig(); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
					}
					interactive.PauseForEnter()
					return nil
				},
			},
		}

		if err := interactive.ShowMainMenu(options); err != nil {
			if errors.Is(err, interactive.ErrExit) {
				fmt.Println("Goodbye!")
				return
			}
			log.Fatal(err)
		}

		fmt.Println()
	}
}

func showNetworkMenu() error {
	for {
		options := []interactive.MenuOption{
			{
				Name:        "Setup",
				Description: "Validate config and setup ClickHouse database (safe to run multiple times)",
				Action: func() error {
					// First show the config and get confirmation
					if err := actions.Setup(true, false); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					// Ask for confirmation
					if !interactive.Confirm("Do you want to proceed with the setup?") {
						fmt.Println("Setup canceled.")
						interactive.PauseForEnter()
						return nil
					}

					// Run the actual setup
					if err := actions.Setup(true, true); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					interactive.PauseForEnter()
					return nil
				},
			},
			{
				Name:        "Teardown",
				Description: "Drop ClickHouse database for the configured network (destructive)",
				Action: func() error {
					// First show the config and get confirmation
					if err := actions.Teardown(true, false); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					// Ask for confirmation
					if !interactive.Confirm("⚠️  Are you SURE you want to drop the database? This cannot be undone!") {
						fmt.Println("Teardown canceled.")
						interactive.PauseForEnter()
						return nil
					}

					// Run the actual teardown
					if err := actions.Teardown(true, true); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					interactive.PauseForEnter()
					return nil
				},
			},
		}

		fmt.Println("\n📁 Network Management")
		fmt.Println("====================")
		if err := interactive.ShowMainMenu(options); err != nil {
			if errors.Is(err, interactive.ErrExit) {
				return nil // Return to main menu
			}
			return err
		}
	}
}

func showTestMenu() error { //nolint:gocyclo // Menu handling requires multiple options
	for {
		options := []interactive.MenuOption{
			{
				Name:        "Run Test",
				Description: "Run a test suite with setup, data ingestion, and assertions",
				Action: func() error {
					// List available tests
					testsDir := "tests"
					entries, err := os.ReadDir(testsDir)
					if err != nil {
						fmt.Printf("\n❌ Error reading tests directory: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					var tests []string
					for _, entry := range entries {
						if entry.IsDir() {
							tests = append(tests, entry.Name())
						}
					}

					if len(tests) == 0 {
						fmt.Println("\n❌ No tests found in tests directory")
						interactive.PauseForEnter()
						return nil
					}

					// Use interactive selection menu
					testName, err := interactive.SelectFromList("Select a test to run:", tests)
					if err != nil {
						fmt.Println("Test selection canceled.")
						interactive.PauseForEnter()
						return nil
					}

					// Check if xatu environment already exists
					xatuExists := false
					if _, err := os.Stat("xatu"); err == nil {
						// Check if xatu-clickhouse containers are running
						cmd := exec.Command("docker", "ps", "--filter", "name=xatu-clickhouse", "--format", "{{.Names}}")
						output, err := cmd.Output()
						if err == nil && strings.TrimSpace(string(output)) != "" {
							xatuExists = true
						}
					}

					// Ask about skipping setup only if xatu exists
					skipSetup := false
					if xatuExists {
						fmt.Println("✅ Detected existing Xatu environment")
						// Default to YES when xatu environment is already running
						skipSetup = interactive.ConfirmWithDefault("Skip xatu setup phase? (use existing xatu environment)", true)
					} else {
						fmt.Println("ℹ️  Xatu environment not detected - will run full setup")
					}

					// Create test service using the shared logger from cmd package
					cfg := test.Config{
						TestsDir:      testsDir,
						XatuRepoURL:   "https://github.com/ethpandaops/xatu",
						XatuRef:       "", // Will use XATU_REF env var or default to master
						Timeout:       10 * time.Minute,
						CheckInterval: 10 * time.Second,
					}

					svc := test.NewService(cmd.Logger, cfg)

					// Start the service
					ctx := context.Background()
					if err := svc.Start(ctx); err != nil {
						fmt.Printf("\n❌ Failed to start test service: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}
					defer func() {
						if err := svc.Stop(); err != nil {
							cmd.Logger.WithError(err).Warn("Failed to stop test service")
						}
					}()

					// Run the test
					fmt.Printf("\n🚀 Running test '%s'...\n", testName)
					if err := svc.RunTest(ctx, testName, skipSetup); err != nil {
						fmt.Printf("\n❌ Test failed: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					fmt.Printf("\n✅ Test '%s' passed successfully!\n", testName)
					interactive.PauseForEnter()
					return nil
				},
			},
			{
				Name:        "Test Teardown",
				Description: "Teardown test environment (stop containers and clean up)",
				Action: func() error {
					if !interactive.Confirm("Are you sure you want to teardown the test environment?") {
						fmt.Println("Teardown canceled.")
						interactive.PauseForEnter()
						return nil
					}

					// Create test service using the shared logger from cmd package
					cfg := test.Config{
						TestsDir: "tests",
					}

					svc := test.NewService(cmd.Logger, cfg)

					// Run teardown
					ctx := context.Background()
					fmt.Println("\n🧹 Running test environment teardown...")
					if err := svc.Teardown(ctx); err != nil {
						fmt.Printf("\n❌ Teardown failed: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					fmt.Println("\n✅ Test environment teardown completed successfully!")
					interactive.PauseForEnter()
					return nil
				},
			},
		}

		fmt.Println("\n🧪 Test Management")
		fmt.Println("==================")
		if err := interactive.ShowMainMenu(options); err != nil {
			if errors.Is(err, interactive.ErrExit) {
				return nil // Return to main menu
			}
			return err
		}
	}
}
