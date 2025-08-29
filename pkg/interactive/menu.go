// Package interactive provides terminal user interface components
package interactive

import (
	"errors"
	"fmt"

	"github.com/AlecAivazis/survey/v2"
)

// MenuOption represents a menu item with its associated action
type MenuOption struct {
	Name        string
	Description string
	Action      func() error
}

var (
	// ErrExit is returned when the user chooses to exit
	ErrExit = errors.New("exit")
	// ErrInvalidSelection is returned when an invalid menu option is selected
	ErrInvalidSelection = errors.New("invalid selection")
)

// ShowMainMenu displays the main menu and handles user selection
func ShowMainMenu(options []MenuOption) error {
	choices := make([]string, 0, len(options)+1)
	optionMap := make(map[string]MenuOption)

	for _, opt := range options {
		choice := fmt.Sprintf("%s - %s", opt.Name, opt.Description)
		choices = append(choices, choice)
		optionMap[choice] = opt
	}

	choices = append(choices, "Exit")

	var selected string
	prompt := &survey.Select{
		Message: "What would you like to do?",
		Options: choices,
	}

	if err := survey.AskOne(prompt, &selected); err != nil {
		return ErrExit
	}

	if selected == "Exit" {
		return ErrExit
	}

	if option, ok := optionMap[selected]; ok {
		return option.Action()
	}

	return ErrInvalidSelection
}

// PauseForEnter waits for the user to press Enter
func PauseForEnter() {
	fmt.Println("\nPress Enter to continue...")
	_, _ = fmt.Scanln()
}

// Confirm asks for user confirmation
func Confirm(message string) bool {
	confirmed := false
	prompt := &survey.Confirm{
		Message: message,
		Default: false,
	}
	_ = survey.AskOne(prompt, &confirmed)
	return confirmed
}
