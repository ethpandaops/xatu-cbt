package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/ethpandaops/xatu-cbt/internal/actions"
	"github.com/ethpandaops/xatu-cbt/internal/interactive"
)

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
				Name:        "Test Spec",
				Description: "Run all tests for a spec (network + fork)",
				Action:      runTestSpecInteractive,
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

func runTestSpecInteractive() error {
	networks := []string{"mainnet", "sepolia"}
	specs := []string{"pectra", "fusaka"}

	network, err := interactive.SelectFromList("Select network:", networks)
	if err != nil {
		fmt.Println("Selection canceled.")
		interactive.PauseForEnter()
		return nil
	}

	spec, err := interactive.SelectFromList("Select spec:", specs)
	if err != nil {
		fmt.Println("Selection canceled.")
		interactive.PauseForEnter()
		return nil
	}

	verbose := interactive.Confirm("Enable verbose output?")

	args := []string{"test", "spec", "--spec", spec, "--network", network}
	if verbose {
		args = append(args, "--verbose")
	}

	return runCLICommand(args...)
}

func runTestModelsInteractive() error {
	networks := []string{"mainnet", "sepolia"}
	specs := []string{"pectra", "fusaka"}

	network, err := interactive.SelectFromList("Select network:", networks)
	if err != nil {
		fmt.Println("Selection canceled.")
		interactive.PauseForEnter()
		return nil
	}

	spec, err := interactive.SelectFromList("Select spec:", specs)
	if err != nil {
		fmt.Println("Selection canceled.")
		interactive.PauseForEnter()
		return nil
	}

	fmt.Print("\nEnter comma-separated model names (e.g., fct_block,canonical_beacon_block): ")
	var models string
	if _, err := fmt.Scanln(&models); err != nil {
		fmt.Println("\n❌ Invalid input")
		interactive.PauseForEnter()
		return nil
	}

	verbose := interactive.Confirm("Enable verbose output?")

	args := []string{"test", "models", models, "--spec", spec, "--network", network}
	if verbose {
		args = append(args, "--verbose")
	}

	return runCLICommand(args...)
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
