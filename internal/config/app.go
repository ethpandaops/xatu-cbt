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

	// Parse numeric values. CBT setup/migrations must target the same host port
	// docker-compose.platform.yml publishes for CBT node 01.
	nativePortValue, nativePortKey := getFirstEnv([]string{ClickHouseCBT01NativePortEnvVar, ClickHouseNativePortEnvVar}, "9000")
	nativePort, err := strconv.Atoi(nativePortValue)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", nativePortKey, err)
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

	port := getCBTNativePort()

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

	port := getXatuNativePort()

	return fmt.Sprintf("clickhouse://%s:%s@%s:%s", username, password, host, port)
}

// GetRedisURL returns the Redis connection URL built from the compose host port.
func GetRedisURL() string {
	return fmt.Sprintf("redis://localhost:%s", getEnv(RedisPortEnvVar, "6380"))
}

// GetProjectName returns the Docker Compose project name, falling back to the default project.
func GetProjectName() string {
	return getEnv(ProjectNameEnvVar, ProjectName)
}

// GetDockerNetworkName returns the Docker Compose-managed network name for the project.
func GetDockerNetworkName() string {
	return fmt.Sprintf("%s_xatu-net", GetProjectName())
}

func getCBTNativePort() string {
	port, _ := getFirstEnv([]string{ClickHouseCBT01NativePortEnvVar, ClickHouseNativePortEnvVar}, "9000")
	return port
}

func getXatuNativePort() string {
	return getEnv(ClickHouseXatu01NativePortEnvVar, "9002")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getFirstEnv(keys []string, defaultValue string) (value, matchedKey string) {
	for _, key := range keys {
		if v := os.Getenv(key); v != "" {
			return v, key
		}
	}

	if len(keys) == 0 {
		return defaultValue, ""
	}

	return defaultValue, keys[0]
}

// parseSafeHostnames parses a comma-separated list of hostnames.
// Handles both quoted and unquoted values from environment variables.
func parseSafeHostnames(s string) []string {
	if s == "" {
		return []string{}
	}

	// Remove surrounding quotes if present (handles both " and ')
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			s = s[1 : len(s)-1]
		}
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
