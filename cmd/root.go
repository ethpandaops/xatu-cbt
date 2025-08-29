package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "xatu-cbt",
	Short: "Xatu CBT - ClickHouse Blockchain Tool",
	Long: `Xatu CBT is a tool for managing and querying blockchain data in ClickHouse.
	
Run without arguments to launch interactive mode, or use subcommands for direct operations.`,
}

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Global flags can be added here if needed
	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is .env)")
}
