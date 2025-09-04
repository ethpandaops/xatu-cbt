package cmd

import (
	"fmt"

	"github.com/savid/xatu-cbt/internal/actions"
	"github.com/spf13/cobra"
)

var showConfigCmd = &cobra.Command{
	Use:   "show-config",
	Short: "Display current environment configuration",
	Long:  `Shows the current configuration loaded from environment variables and .env file.`,
	RunE: func(_ *cobra.Command, _ []string) error {
		if err := actions.ShowConfig(); err != nil {
			return fmt.Errorf("failed to show config: %w", err)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(showConfigCmd)
}
