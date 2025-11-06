package cmd

import (
	"fmt"

	"github.com/ethpandaops/xatu-cbt/internal/testing/xatu"
	"github.com/sirupsen/logrus"
)

// newLogger creates a new logger with the appropriate log level based on the verbose flag.
// If verbose is true, the logger is set to DebugLevel, otherwise InfoLevel.
func newLogger(verbose bool) *logrus.Logger {
	log := logrus.New()
	if verbose {
		log.SetLevel(logrus.DebugLevel)
	} else {
		log.SetLevel(logrus.InfoLevel)
	}
	return log
}

// ensureXatuRepo ensures the xatu repository exists and returns its path.
func ensureXatuRepo(log logrus.FieldLogger, wd, repoURL, ref string) (string, error) {
	xatuRepoManager := xatu.NewRepoManager(log, wd, repoURL, ref)
	repoPath, err := xatuRepoManager.EnsureRepo()
	if err != nil {
		return "", fmt.Errorf("ensuring xatu repository: %w", err)
	}
	return repoPath, nil
}
