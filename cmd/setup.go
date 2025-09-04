package cmd

import (
	"fmt"

	"github.com/savid/xatu-cbt/internal/actions"
	"github.com/spf13/cobra"
)

var (
	forceSetup bool
)

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Setup ClickHouse database for the configured network",
	Long: `Validates configuration and sets up ClickHouse database for the configured network.
This command will:
- Validate your configuration
- Test the ClickHouse connection
- Create the network database if it doesn't exist
- Run database migrations`,
	RunE: func(_ *cobra.Command, _ []string) error {
		// For CLI mode, we pass skipConfirm=true if --force is used
		// Otherwise the action will just show the config and return
		if !forceSetup {
			// First call to show config
			if err := actions.Setup(false, false); err != nil {
				return err
			}
			fmt.Println("\nUse --force flag to proceed with setup")
			return nil
		}

		// Run the actual setup
		if err := actions.Setup(false, true); err != nil {
			return fmt.Errorf("setup failed: %w", err)
		}
		return nil
	},
}

func init() {
	setupCmd.Flags().BoolVarP(&forceSetup, "force", "f", false, "Skip confirmation and proceed with setup")
	// Command is added to networkCmd in network.go
}
