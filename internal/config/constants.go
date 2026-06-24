package config

const (
	// XatuClusterName is the ClickHouse cluster name for external data.
	XatuClusterName = "xatu_cluster"
	// CBTClusterName is the ClickHouse cluster name for transformations.
	// Topology: 2 shards x 1 replica — sharded (mirrors prod cross-shard behavior) but
	// without a 2nd replica per shard, which would double ON CLUSTER DDL cost and time
	// the test suite out.
	CBTClusterName = "cluster_2S_1R"
	// ClickHouseLocalSuffix is the suffix appended to distributed table names to access local shards.
	ClickHouseLocalSuffix = "_local"
	// RedisContainerName is the Redis Compose service name.
	RedisContainerName = "xatu-cbt-redis"
	// RedisContainerPort is the internal port Redis listens on within the container.
	RedisContainerPort = "6379"
	// ClickHouseContainer is the CBT ClickHouse node 01 Compose service name.
	ClickHouseContainer = "xatu-cbt-clickhouse-01"
	// ClickHouseContainerHTTPPort is the internal HTTP port ClickHouse listens on within the container.
	ClickHouseContainerHTTPPort = "8123"
	// ClickHouseContainerNativePort is the internal native port ClickHouse listens on within the container.
	ClickHouseContainerNativePort = "9000"
	// ClickHouseNativePortEnvVar is the legacy ClickHouse native port environment variable.
	ClickHouseNativePortEnvVar = "CLICKHOUSE_NATIVE_PORT"
	// ClickHouseCBT01NativePortEnvVar is the CBT ClickHouse node 01 native host port environment variable.
	ClickHouseCBT01NativePortEnvVar = "CLICKHOUSE_CBT_01_NATIVE_PORT"
	// ClickHouseXatu01NativePortEnvVar is the Xatu ClickHouse node 01 native host port environment variable.
	ClickHouseXatu01NativePortEnvVar = "CLICKHOUSE_XATU_01_NATIVE_PORT"
	// RedisPortEnvVar is the Redis host port environment variable.
	RedisPortEnvVar = "REDIS_PORT"
	// PlatformComposeFile is the Docker Compose file for the platform infrastructure.
	PlatformComposeFile = "docker-compose.platform.yml"
	// ProjectName is the Docker Compose project name.
	ProjectName = "xatu-cbt-platform"
	// ProjectNameEnvVar is the environment variable used to override the Docker Compose project name.
	ProjectNameEnvVar = "XATU_CBT_PROJECT_NAME"
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
	// DefaultDatabase is the name of the default database (used as xatu template).
	DefaultDatabase = "default"
	// CBTTemplateDatabase is the name of the CBT template database for cloning.
	CBTTemplateDatabase = "cbt_template"
	// ExternalDBPrefix is the prefix for per-test external databases.
	ExternalDBPrefix = "ext_"
	// CBTDBPrefix is the prefix for per-test CBT databases.
	CBTDBPrefix = "cbt_"
	// XatuRepoURL is the URL of the xatu repository.
	XatuRepoURL = "https://github.com/ethpandaops/xatu"
	// XatuDefaultRef is the default git reference for the xatu repository.
	XatuDefaultRef = "master"
	// XatuMigrationsPath is the path to migrations within the xatu repository.
	// Each per-schema set lives in its own subdirectory below this path.
	XatuMigrationsPath = "deploy/migrations/clickhouse"
	// DefaultRedisURL is the default redis url.
	DefaultRedisURL = "redis://localhost:6380"
)

// XatuMigrationSet pairs a xatu migration set (a subdirectory under
// XatuMigrationsPath) with the ClickHouse database it targets. xatu splits its
// migrations into database-agnostic per-schema sets; each set is applied to its
// target database and tracked in its own schema_migrations_<Name> table.
type XatuMigrationSet struct {
	// Name is the set's subdirectory name and the schema_migrations_<Name> suffix.
	Name string
	// Database is the ClickHouse database the set is applied to.
	Database string
}

// XatuMigrationSets returns xatu's per-schema migration sets and their target
// databases for the local stack. Mirrors xatu's clickhouse-migrate.sh convention
// (directory name == database name), with the xatu set remapped to `default`.
func XatuMigrationSets() []XatuMigrationSet {
	return []XatuMigrationSet{
		{Name: "xatu", Database: DefaultDatabase},
		{Name: "observoor", Database: "observoor"},
		{Name: "admin", Database: "admin"},
	}
}
