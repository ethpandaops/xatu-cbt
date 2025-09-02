// Package main is the entry point for the xatu-cbt application
package main

import (
	"os"

	"github.com/savid/xatu-cbt/cmd"
)

func main() {
	// Mode detection: if no arguments, run TUI, otherwise run CLI
	if len(os.Args) == 1 {
		// No arguments provided - run interactive TUI mode
		runInteractive()
	} else {
		// Arguments provided - run cobra CLI
		cmd.Execute()
	}
}
