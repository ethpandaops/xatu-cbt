package cmd

import (
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
