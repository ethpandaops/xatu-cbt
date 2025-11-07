// Package config handles configuration loading and management
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// AppConfig holds the application deployment configuration loaded from environment variables.
type AppConfig struct {
	Network                  string
	ClickhouseHost           string
	ClickhouseNativePort     int
	ClickhouseDataIngestPort int
	ClickhouseUsername       string
	ClickhousePassword       string
	ClickhouseCluster        string
	SafeHostnames            []string
}

// Load reads configuration from environment variables and .env file.
func Load() (*AppConfig, error) {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		// It's okay if the file doesn't exist
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("error loading .env file: %w", err)
		}
	}

	// Parse safe hostnames from comma-separated env var
	safeHostnamesStr := getEnv("XATU_CBT_SAFE_HOSTS", "")
	safeHostnames := parseSafeHostnames(safeHostnamesStr)

	cfg := &AppConfig{
		Network:            getEnv("NETWORK", "mainnet"),
		ClickhouseHost:     getEnv("CLICKHOUSE_HOST", "localhost"),
		ClickhouseUsername: getEnv("CLICKHOUSE_USERNAME", "default"),
		ClickhousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		ClickhouseCluster:  getEnv("CLICKHOUSE_CLUSTER", ""),
		SafeHostnames:      safeHostnames,
	}

	// Parse numeric values
	nativePort, err := strconv.Atoi(getEnv("CLICKHOUSE_NATIVE_PORT", "9000"))
	if err != nil {
		return nil, fmt.Errorf("invalid CLICKHOUSE_NATIVE_PORT: %w", err)
	}
	cfg.ClickhouseNativePort = nativePort

	// Parse data ingest port (used only for test data loading)
	dataIngestPort, err := strconv.Atoi(getEnv("CLICKHOUSE_01_NATIVE_PORT", "9000"))
	if err != nil {
		return nil, fmt.Errorf("invalid CLICKHOUSE_01_NATIVE_PORT: %w", err)
	}
	cfg.ClickhouseDataIngestPort = dataIngestPort

	return cfg, nil
}

func (c *AppConfig) String() string {
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

// GetCBTClickHouseURL returns the CBT ClickHouse connection URL built from environment variables.
// This reads from the same env vars that docker-compose uses, ensuring consistency.
func GetCBTClickHouseURL() string {
	username := os.Getenv("CLICKHOUSE_USERNAME")
	if username == "" {
		username = "default"
	}

	password := os.Getenv("CLICKHOUSE_PASSWORD")
	if password == "" {
		password = "supersecret"
	}

	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost"
	}

	// Check specific port for CBT cluster, then generic, then default
	port := os.Getenv("CLICKHOUSE_CBT_01_NATIVE_PORT")
	if port == "" {
		port = os.Getenv("CLICKHOUSE_NATIVE_PORT")
	}
	if port == "" {
		port = "9000"
	}

	return fmt.Sprintf("clickhouse://%s:%s@%s:%s", username, password, host, port)
}

// GetXatuClickHouseURL returns the Xatu ClickHouse connection URL built from environment variables.
func GetXatuClickHouseURL() string {
	username := os.Getenv("CLICKHOUSE_USERNAME")
	if username == "" {
		username = "default"
	}

	password := os.Getenv("CLICKHOUSE_PASSWORD")
	if password == "" {
		password = "supersecret"
	}

	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost"
	}

	// Check specific port for Xatu cluster, then generic, then default
	port := os.Getenv("CLICKHOUSE_XATU_01_NATIVE_PORT")
	if port == "" {
		port = os.Getenv("CLICKHOUSE_NATIVE_PORT")
	}
	if port == "" {
		port = "9002"
	}

	return fmt.Sprintf("clickhouse://%s:%s@%s:%s", username, password, host, port)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// parseSafeHostnames parses a comma-separated list of hostnames.
func parseSafeHostnames(s string) []string {
	if s == "" {
		return []string{}
	}

	parts := strings.Split(s, ",")
	hostnames := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			hostnames = append(hostnames, trimmed)
		}
	}

	return hostnames
}
