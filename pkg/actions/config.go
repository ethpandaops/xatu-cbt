package actions

import (
	"fmt"

	"github.com/savid/xatu-cbt/pkg/config"
)

// ShowConfig displays the current configuration
func ShowConfig() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	fmt.Println(cfg.String())
	return nil
}
