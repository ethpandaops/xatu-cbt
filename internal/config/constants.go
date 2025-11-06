package config

import (
	"fmt"
	"os"
)

const (
	// ClickHouse cluster names
	XatuClusterName = "xatu_cluster"  // 1 shard, 2 replicas (external data)
	CBTClusterName  = "cluster_2S_1R" // 2 shards, 1 replica (transformations)

	// Docker
	DockerNetwork       = "xatu_xatu-net"
	CBTDockerImage      = "ethpandaops/cbt:debian-latest"
	RedisContainerName  = "xatu-cbt-redis"
	ClickHouseContainer = "xatu-cbt-clickhouse-01"

	// Docker Compose
	PlatformComposeFile = "docker-compose.platform.yml"
	ProjectName         = "xatu-cbt-platform"

	// File paths (relative to project root)
	ModelsExternalDir       = "models/external"
	ModelsTransformationsDir = "models/transformations"
	ModelsDir               = "models"
	MigrationsDir           = "migrations"
	TestsDir                = "tests"

	// Database
	TestDatabasePrefix     = "test_"
	SchemaMigrationsPrefix = "schema_migrations_"
	DefaultDatabase        = "default"

	// Xatu repository
	XatuRepoURL        = "https://github.com/ethpandaops/xatu"
	XatuDefaultRef     = "master"
	XatuMigrationsPath = "deploy/migrations/clickhouse"
)

// Default connection strings
const (
	DefaultRedisURL = "redis://localhost:6380"
)

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
