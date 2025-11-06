package config

const (
	// XatuClusterName is the ClickHouse cluster name for external data.
	XatuClusterName = "xatu_cluster"
	// CBTClusterName is the ClickHouse cluster name for transformations.
	CBTClusterName = "cluster_2S_1R"
	// ClickHouseLocalSuffix is the suffix appended to distributed table names to access local shards.
	ClickHouseLocalSuffix = "_local"
	// RedisContainerName is the name of the Redis container.
	RedisContainerName = "xatu-cbt-redis"
	// RedisContainerPort is the internal port Redis listens on within the container.
	RedisContainerPort = "6379"
	// ClickHouseContainer is the name of the ClickHouse container.
	ClickHouseContainer = "xatu-cbt-clickhouse-01"
	// ClickHouseContainerHTTPPort is the internal HTTP port ClickHouse listens on within the container.
	ClickHouseContainerHTTPPort = "8123"
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
