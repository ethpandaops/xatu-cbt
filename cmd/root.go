package cmd

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// Logger is the shared logger instance for all commands
	Logger *logrus.Logger

	rootCmd = &cobra.Command{
		Use:   "xatu-cbt",
		Short: "Xatu CBT - ClickHouse Blockchain Tool",
		Long: `Xatu CBT is a tool for managing and querying blockchain data in ClickHouse.
	
Run without arguments to launch interactive mode, or use subcommands for direct operations.`,
	}
)

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Load .env file if it exists
	_ = godotenv.Load()

	// Initialize the shared logger
	Logger = logrus.New()

	// Set log level from environment variable
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info" // Default to info
	}

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		// Can't use Logger here since it might not be set up yet
		fmt.Printf("Invalid LOG_LEVEL '%s', defaulting to 'info'\n", logLevel)
		level = logrus.InfoLevel
	}
	Logger.SetLevel(level)
}
