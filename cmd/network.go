package cmd

import (
	"github.com/spf13/cobra"
)

var networkCmd = &cobra.Command{
	Use:   "network",
	Short: "Network database management commands",
	Long:  `Commands for managing network databases including setup and teardown operations.`,
}

func init() {
	networkCmd.AddCommand(setupCmd)
	networkCmd.AddCommand(teardownCmd)
	rootCmd.AddCommand(networkCmd)
}
