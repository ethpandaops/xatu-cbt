package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ethpandaops/xatu-cbt/internal/actions"
	"github.com/ethpandaops/xatu-cbt/internal/config"
	"github.com/ethpandaops/xatu-cbt/internal/interactive"
	"github.com/ethpandaops/xatu-cbt/internal/testing"
	"github.com/sirupsen/logrus"
)

var (
	errNoModelsFound   = errors.New("no models found")
	errNoNetworksFound = errors.New("no networks found in tests/ directory")
)

// discoverNetworks returns a sorted list of available networks from the tests directory
func discoverNetworks() ([]string, error) {
	testsDir := "tests"
	entries, err := os.ReadDir(testsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read tests directory: %w", err)
	}

	networks := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			networks = append(networks, entry.Name())
		}
	}

	sort.Strings(networks)
	return networks, nil
}

func runInteractive() {
	fmt.Println("Xatu CBT - Interactive Mode")
	fmt.Println("===========================")
	fmt.Println()

	for {
		options := []interactive.MenuOption{
			{
				Name:        "🧪 Test Management",
				Description: "Run tests and manage test infrastructure",
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
					if err := actions.Setup(true, false); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					if !interactive.Confirm("Do you want to proceed with the setup?") {
						fmt.Println("Setup canceled.")
						interactive.PauseForEnter()
						return nil
					}

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
					if err := actions.Teardown(true, false); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					if !interactive.Confirm("⚠️  Are you SURE you want to drop the database? This cannot be undone!") {
						fmt.Println("Teardown canceled.")
						interactive.PauseForEnter()
						return nil
					}

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

func showTestMenu() error {
	for {
		options := []interactive.MenuOption{
			{
				Name:        "Infrastructure Management",
				Description: "Start, stop, and manage test infrastructure",
				Action:      showInfraMenu,
			},
			{
				Name:        "Test All",
				Description: "Run all tests for a network",
				Action:      runTestAllInteractive,
			},
			{
				Name:        "Test Models",
				Description: "Run tests for specific models",
				Action:      runTestModelsInteractive,
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

func showInfraMenu() error {
	for {
		options := []interactive.MenuOption{
			{
				Name:        "Start",
				Description: "Start platform infrastructure (ClickHouse, Redis, Zookeeper)",
				Action: func() error {
					return runCLICommand("infra", "start")
				},
			},
			{
				Name:        "Stop",
				Description: "Stop platform infrastructure (preserves volumes)",
				Action: func() error {
					return runCLICommand("infra", "stop")
				},
			},
			{
				Name:        "Status",
				Description: "Check platform status and health",
				Action: func() error {
					return runCLICommand("infra", "status", "--verbose")
				},
			},
			{
				Name:        "Reset",
				Description: "Reset platform (removes all volumes for fresh start)",
				Action: func() error {
					if !interactive.Confirm("⚠️  Are you SURE you want to reset the platform? This will remove all volumes!") {
						fmt.Println("Reset canceled.")
						interactive.PauseForEnter()
						return nil
					}
					return runCLICommand("infra", "reset")
				},
			},
		}

		fmt.Println("\n🏗️  Infrastructure Management")
		fmt.Println("============================")
		if err := interactive.ShowMainMenu(options); err != nil {
			if errors.Is(err, interactive.ErrExit) {
				return nil // Return to test menu
			}
			return err
		}
	}
}

// selectNetwork prompts the user to select a network.
func selectNetwork() (string, error) {
	networks, err := discoverNetworks()
	if err != nil {
		return "", fmt.Errorf("failed to discover networks: %w", err)
	}

	if len(networks) == 0 {
		return "", errNoNetworksFound
	}

	if len(networks) == 1 {
		fmt.Printf("Auto-selected network: %s\n", networks[0])
		return networks[0], nil
	}

	network, err := interactive.SelectFromList("Select network:", networks)
	if err != nil {
		return "", err
	}

	return network, nil
}

func runTestAllInteractive() error {
	network, err := selectNetwork()
	if err != nil {
		fmt.Printf("❌ %v\n", err)
		interactive.PauseForEnter()
		return nil
	}

	verbose := interactive.ConfirmWithDefault("Enable verbose output?", true)

	args := []string{"test", "all", "--network", network}
	if verbose {
		args = append(args, "--verbose")
	}

	return runCLICommand(args...)
}

// initializeAndLoadModels initializes the model cache and loads all available models.
func initializeAndLoadModels() ([]string, error) {
	wd, wdErr := os.Getwd()
	if wdErr != nil {
		return nil, fmt.Errorf("failed to get working directory: %w", wdErr)
	}

	// Create a simple logger for model cache (only show errors)
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	logger.SetOutput(os.Stderr)

	modelCache := testing.NewModelCache(logger)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("Loading available models...")
	if loadErr := modelCache.LoadAll(
		ctx,
		filepath.Join(wd, config.ModelsExternalDir),
		filepath.Join(wd, config.ModelsTransformationsDir),
	); loadErr != nil {
		return nil, fmt.Errorf("failed to load models: %w", loadErr)
	}

	// Get all models and sort them alphabetically
	allModels := modelCache.ListAllModels()
	sort.Strings(allModels)

	if len(allModels) == 0 {
		return nil, errNoModelsFound
	}

	return allModels, nil
}

// promptForNextAction prompts the user for what to do after a test run.
func promptForNextAction() (string, error) {
	choices := []string{
		"Test again (same settings)",
		"Test with different settings",
		"Return to menu",
	}

	return interactive.SelectFromList("\nWhat would you like to do?", choices)
}

func runTestModelsInteractive() error {
	// Initialize and load models
	allModels, err := initializeAndLoadModels()
	if err != nil {
		fmt.Printf("❌ %v\n", err)
		interactive.PauseForEnter()
		return nil
	}

	// Loop to allow repeated testing with the same or different parameters
	for {
		// Select network
		network, networkErr := selectNetwork()
		if networkErr != nil {
			fmt.Printf("❌ %v\n", networkErr)
			interactive.PauseForEnter()
			return nil
		}

		// Use multi-select for models
		selectedModels, selectErr := interactive.MultiSelectFromList(
			"Select models to test (space to select, / to search, enter to confirm):",
			allModels,
		)
		if selectErr != nil {
			fmt.Println("Selection canceled.")
			interactive.PauseForEnter()
			return nil
		}

		if len(selectedModels) == 0 {
			fmt.Println("\n❌ No models selected")
			interactive.PauseForEnter()
			continue
		}

		verbose := interactive.ConfirmWithDefault("Enable verbose output?", true)

		// Join selected models with commas
		modelsArg := strings.Join(selectedModels, ",")
		args := []string{"test", "models", modelsArg, "--network", network}
		if verbose {
			args = append(args, "--verbose")
		}

		// Inner loop for re-running the same test
		for {
			// Run the test (errors are displayed by runCLICommand)
			_ = runCLICommand(args...)

			// Ask user what to do next
			nextAction, actionErr := promptForNextAction()
			if actionErr != nil || nextAction == "Return to menu" {
				return nil
			}

			if nextAction == "Test again (same settings)" {
				fmt.Println() // Add blank line for readability
				continue      // Re-run with same args
			}

			// "Test with different settings" - break inner loop to prompt for new settings
			fmt.Println() // Add blank line for readability
			break
		}
	}
}

func runCLICommand(args ...string) error {
	// Get the binary path - should be in ./bin/xatu-cbt
	binaryPath := filepath.Join(".", "bin", "xatu-cbt")

	// Check if binary exists
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		fmt.Printf("\n❌ Binary not found at %s\n", binaryPath)
		fmt.Println("Please run 'make' to build the binary first.")
		interactive.PauseForEnter()
		return nil
	}

	fmt.Printf("\n🚀 Running: %s %v\n\n", binaryPath, args)

	// #nosec G204 -- binaryPath is hardcoded to ./bin/xatu-cbt and args are controlled by menu selections
	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		fmt.Printf("\n❌ Command failed: %v\n", err)
		interactive.PauseForEnter()
		return nil
	}

	interactive.PauseForEnter()
	return nil
}
