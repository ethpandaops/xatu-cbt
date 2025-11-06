package config

import (
	"fmt"
	"os"
)

const (
	// XatuClusterName is the ClickHouse cluster name for external data (1 shard, 2 replicas).
	XatuClusterName = "xatu_cluster"
	// CBTClusterName is the ClickHouse cluster name for transformations (2 shards, 1 replica).
	CBTClusterName = "cluster_2S_1R"
	// RedisContainerName is the name of the Redis container.
	RedisContainerName = "xatu-cbt-redis"
	// ClickHouseContainer is the name of the ClickHouse container.
	ClickHouseContainer = "xatu-cbt-clickhouse-01"
	// PlatformComposeFile is the Docker Compose file for the platform infrastructure.
	PlatformComposeFile = "docker-compose.platform.yml"
	// ProjectName is the Docker Compose project name.
	ProjectName = "xatu-cbt-platform"
	// ModelsExternalDir is the directory path for external models.
	ModelsExternalDir = "models/external"
	// ModelsTransformationsDir is the directory path for transformation models.
	ModelsTransformationsDir = "models/transformations"
	// ModelsDir is the base models directory path.
	ModelsDir = "models"
	// MigrationsDir is the directory path for database migrations.
	MigrationsDir = "migrations"
	// TestsDir is the directory path for tests.
	TestsDir = "tests"
	// SchemaMigrationsPrefix is the prefix used for schema migration tables.
	SchemaMigrationsPrefix = "schema_migrations_"
	// DefaultDatabase is the name of the default database.
	DefaultDatabase = "default"
	// XatuRepoURL is the URL of the xatu repository.
	XatuRepoURL = "https://github.com/ethpandaops/xatu"
	// XatuDefaultRef is the default git reference for the xatu repository.
	XatuDefaultRef = "master"
	// XatuMigrationsPath is the path to migrations within the xatu repository.
	XatuMigrationsPath = "deploy/migrations/clickhouse"
	// DefaultRedisURL is the default redis url.
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
