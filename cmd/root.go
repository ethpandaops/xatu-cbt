package cmd

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// Logger is xatu-cbt global logger instance.
	Logger  *logrus.Logger
	envFile string
	rootCmd = &cobra.Command{
		Use:   "xatu-cbt",
		Short: "Xatu CBT - ClickHouse Blockchain Tool",
		Long: `Xatu CBT is a tool for managing and querying blockchain data in ClickHouse.

Run without arguments to launch interactive mode, or use subcommands for direct operations.`,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			return loadEnvFile(envFile)
		},
	}
)

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// InitLogger initializes or reinitializes the logger based on environment variables.
func InitLogger() {
	if Logger == nil {
		Logger = logrus.New()
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		fmt.Printf("Invalid LOG_LEVEL '%s', defaulting to 'info'\n", logLevel)
		level = logrus.InfoLevel
	}
	Logger.SetLevel(level)
}

func init() {
	rootCmd.PersistentFlags().StringVar(&envFile, "env", "", "Path to environment file (default: .env)")

	// Initialize logger with defaults - will be reinitialized after env file is loaded
	InitLogger()

	rootCmd.AddCommand(testCmd)
	rootCmd.AddCommand(infraCmd)
}

// loadEnvFile loads the specified environment file.
func loadEnvFile(file string) error {
	if file == "" {
		file = ".env"
	}

	if err := godotenv.Load(file); err != nil {
		// If it's the default .env file and it doesn't exist, that's okay
		if file == ".env" && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to load env file '%s': %w", file, err)
	}

	InitLogger()
	return nil
}
