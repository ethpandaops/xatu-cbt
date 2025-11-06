package config

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
	DefaultXatuClickHouseURL = "clickhouse://default:supersecret@localhost:9002"
	DefaultCBTClickHouseURL  = "clickhouse://localhost:9000"
	DefaultRedisURL          = "redis://localhost:6380"
)
