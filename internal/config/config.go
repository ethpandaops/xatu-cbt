// Package config handles configuration loading and management
package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds the application configuration
type Config struct {
	Network              string
	ClickhouseHost       string
	ClickhouseNativePort int
	ClickhouseUsername   string
	ClickhousePassword   string
	ClickhouseCluster    string
}

// Load reads configuration from environment variables and .env file
func Load() (*Config, error) {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		// It's okay if the file doesn't exist
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("error loading .env file: %w", err)
		}
	}

	cfg := &Config{
		Network:            getEnv("NETWORK", "mainnet"),
		ClickhouseHost:     getEnv("CLICKHOUSE_HOST", "localhost"),
		ClickhouseUsername: getEnv("CLICKHOUSE_USERNAME", "default"),
		ClickhousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		ClickhouseCluster:  getEnv("CLICKHOUSE_CLUSTER", ""),
	}

	// Parse numeric values
	nativePort, err := strconv.Atoi(getEnv("CLICKHOUSE_NATIVE_PORT", "9000"))
	if err != nil {
		return nil, fmt.Errorf("invalid CLICKHOUSE_NATIVE_PORT: %w", err)
	}
	cfg.ClickhouseNativePort = nativePort

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func (c *Config) String() string {
	passwordDisplay := "(not set)"
	if c.ClickhousePassword != "" {
		passwordDisplay = "********"
	}

	clusterDisplay := c.ClickhouseCluster
	if clusterDisplay == "" {
		clusterDisplay = "(single-node)"
	}

	return fmt.Sprintf(`Current Configuration:
======================
Network:                %s
ClickHouse Host:        %s
ClickHouse Native Port: %d
ClickHouse Username:    %s
ClickHouse Password:    %s
ClickHouse Cluster:     %s`,
		c.Network,
		c.ClickhouseHost,
		c.ClickhouseNativePort,
		c.ClickhouseUsername,
		passwordDisplay,
		clusterDisplay,
	)
}
