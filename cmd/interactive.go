// Package cmd contains CLI command definitions
package cmd

import (
	"errors"
	"fmt"
	"log"

	"github.com/savid/xatu-cbt/pkg/actions"
	"github.com/savid/xatu-cbt/pkg/interactive"
	"github.com/spf13/cobra"
)

var interactiveCmd = &cobra.Command{
	Use:   "interactive",
	Short: "Launch interactive TUI mode",
	Long:  `Launches the interactive Terminal User Interface for Xatu CBT.`,
	Run: func(_ *cobra.Command, _ []string) {
		runInteractiveTUI()
	},
}

func init() {
	rootCmd.AddCommand(interactiveCmd)
}

func runInteractiveTUI() {
	fmt.Println("Xatu CBT - Interactive Mode")
	fmt.Println("===========================")
	fmt.Println()

	for {
		options := []interactive.MenuOption{
			{
				Name:        "Show Config",
				Description: "Display current environment configuration",
				Action: func() error {
					if err := actions.ShowConfig(); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
					}
					interactive.PauseForEnter()
					return nil
				},
			},
			{
				Name:        "Setup",
				Description: "Validate config and setup ClickHouse database (safe to run multiple times)",
				Action: func() error {
					// First show the config and get confirmation
					if err := actions.Setup(true, false); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					// Ask for confirmation
					if !interactive.Confirm("Do you want to proceed with the setup?") {
						fmt.Println("Setup canceled.")
						interactive.PauseForEnter()
						return nil
					}

					// Run the actual setup
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
					// First show the config and get confirmation
					if err := actions.Teardown(true, false); err != nil {
						fmt.Printf("\n❌ Error: %v\n", err)
						interactive.PauseForEnter()
						return nil
					}

					// Ask for confirmation
					if !interactive.Confirm("⚠️  Are you SURE you want to drop the database? This cannot be undone!") {
						fmt.Println("Teardown canceled.")
						interactive.PauseForEnter()
						return nil
					}

					// Run the actual teardown
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
