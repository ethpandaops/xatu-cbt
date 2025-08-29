package cmd

import (
	"fmt"

	"github.com/savid/xatu-cbt/pkg/actions"
	"github.com/spf13/cobra"
)

var (
	forceTeardown bool
)

var teardownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Teardown ClickHouse database for the configured network",
	Long: `Validates configuration and drops the ClickHouse database for the configured network.
This command will:
- Validate your configuration
- Test the ClickHouse connection
- DROP the network database and all its data

⚠️  WARNING: This will permanently delete all data in the database!`,
	RunE: func(_ *cobra.Command, _ []string) error {
		// For CLI mode, we pass skipConfirm=true if --force is used
		// Otherwise the action will just show the config and return
		if !forceTeardown {
			// First call to show config
			if err := actions.Teardown(false, false); err != nil {
				return err
			}
			fmt.Println("\n⚠️  WARNING: This will permanently delete all data in the database!")
			fmt.Println("Use --force flag to proceed with teardown")
			return nil
		}

		// Run the actual teardown
		if err := actions.Teardown(false, true); err != nil {
			return fmt.Errorf("teardown failed: %w", err)
		}
		return nil
	},
}

func init() {
	teardownCmd.Flags().BoolVarP(&forceTeardown, "force", "f", false, "Skip confirmation and proceed with teardown")
	rootCmd.AddCommand(teardownCmd)
}
