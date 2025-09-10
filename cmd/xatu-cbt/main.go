// Package main is the entry point for the xatu-cbt application
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/ethpandaops/xatu-cbt/cmd"
	"github.com/joho/godotenv"
)

const (
	envFlag      = "--env"
	envFlagEqual = "--env="
)

func main() {
	// Parse --env flag and determine mode
	envFile, runTUI := parseArgs()

	if runTUI {
		// Load env file for TUI mode
		if err := loadEnvFile(envFile); err != nil {
			fmt.Fprintf(os.Stderr, "Error loading env file: %v\n", err)
			os.Exit(1)
		}
		// Initialize cmd.Logger after loading env file
		cmd.InitLogger()
		// Run interactive TUI mode
		runInteractive()
	} else {
		// Arguments provided - run cobra CLI (it will handle --env flag itself)
		cmd.Execute()
	}
}

// parseArgs parses command line arguments to extract env file and determine run mode
func parseArgs() (envFile string, runTUI bool) {
	// Parse --env flag manually
	for i, arg := range os.Args {
		if arg == envFlag && i+1 < len(os.Args) {
			envFile = os.Args[i+1]
			break
		}
		if strings.HasPrefix(arg, envFlagEqual) {
			envFile = arg[len(envFlagEqual):]
			break
		}
	}

	// Determine if we should run TUI or CLI
	switch len(os.Args) {
	case 1:
		// No arguments - run TUI
		return envFile, true
	case 2:
		// Single argument
		if os.Args[1] == envFlag {
			// --env without value, error
			fmt.Fprintln(os.Stderr, "Error: --env flag requires a value")
			os.Exit(1)
		}
		if strings.HasPrefix(os.Args[1], envFlagEqual) {
			// Only --env=value provided, run TUI
			return envFile, true
		}
		// Other argument, run CLI
		return envFile, false
	case 3:
		// Two arguments
		if os.Args[1] == envFlag {
			// Only --env value provided, run TUI
			return envFile, true
		}
		// Other arguments, run CLI
		return envFile, false
	default:
		// More than 2 arguments - definitely CLI
		return envFile, false
	}
}

// loadEnvFile loads the specified environment file
func loadEnvFile(file string) error {
	if file == "" {
		file = ".env"
	}

	// Try to load the specified env file
	if err := godotenv.Load(file); err != nil {
		// If it's the default .env file and it doesn't exist, that's okay
		if file == ".env" && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to load env file '%s': %w", file, err)
	}

	return nil
}
